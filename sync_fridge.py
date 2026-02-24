#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_fridge.py
- Google Sheets: Inventory / Pantry / Settings / DailyMenus
- Modes: menu | estimate | recipe
- LLM: gpt-4o-mini (JSON output)
- Trigger: schedule | telegram

Required env vars (recommended):
  GOOGLE_SA_JSON          : service account json (raw json string) OR base64 (see GOOGLE_SA_JSON_B64)
  GOOGLE_SA_JSON_B64      : base64-encoded service account json (optional)
  GOOGLE_SHEET_ID         : spreadsheet id
  OPENAI_API_KEY          : OpenAI API key
  TELEGRAM_BOT_TOKEN      : telegram bot token (optional if you don't want to send)
  TELEGRAM_CHAT_ID        : telegram chat id (optional)

Optional:
  TZ                      : default "Asia/Seoul"
  OPENAI_MODEL            : default "gpt-4o-mini"
"""

from __future__ import annotations

import argparse
import base64
import dataclasses
import datetime as dt
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

# ---- Google Sheets ----
import gspread
from google.oauth2.service_account import Credentials

# ---- OpenAI ----
from openai import OpenAI  # openai>=1.x

# ---- Telegram ----
import urllib.request
import urllib.parse


# =========================
# Constants / Helpers
# =========================

TZ_NAME = os.getenv("TZ", "Asia/Seoul")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

REQUIRED_TABS = ["Inventory", "Pantry", "Settings", "DailyMenus"]

INVENTORY_COLUMNS = [
    "Item",
    "Qty",
    "Unit",
    "Storage (Fridge/Freezer/Pantry)",
    "PurchasedDate (YYYY-MM-DD)",
    "UseBy_Est (YYYY-MM-DD)",
    "UseBy_Source (llm/user)",
    "UseBy_Confidence (high/medium/low)",
    "UseBy_Basis",
    "Category",
    "Notes",
]

PANTRY_COLUMNS = [
    "Item",
    "Qty",
    "Unit",
    "UseBy_Est",
    "UseBy_Source",
    "UseBy_Confidence",
    "UseBy_Basis",
    "Notes",
]

SETTINGS_COLUMNS = [
    "Concept",
    "DefaultServings",
    "ExpiringSoonDays",
    "DailyMenuCount",
    "AutoEstimateUseBy",
    "MaxEstimatePerRun",
    "OverwriteLLMEstimates",
]

DAILYMENUS_COLUMNS = [
    "Date",
    "Mode (menu/recipe/estimate)",
    "MenusJSON",
    "AlertsJSON",
    "RequestedServings",
    "ConceptUsed",
    "CreatedAt",
    "Trigger (schedule/telegram)",
]


def die(msg: str, code: int = 1) -> None:
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)


def utcnow_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def today_yyyy_mm_dd_kst() -> str:
    # Minimal TZ handling without external deps:
    # Treat KST as UTC+9 (fixed offset).
    now_utc = dt.datetime.utcnow()
    now_kst = now_utc + dt.timedelta(hours=9)
    return now_kst.date().isoformat()


def parse_yyyy_mm_dd(s: str) -> Optional[dt.date]:
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None


def to_iso(d: dt.date) -> str:
    return d.isoformat()


def normalize_yes_no(v: Any, default: str = "N") -> str:
    if v is None:
        return default
    s = str(v).strip().upper()
    if s in ("Y", "YES", "TRUE", "1"):
        return "Y"
    if s in ("N", "NO", "FALSE", "0"):
        return "N"
    return default


def safe_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        return int(str(v).strip())
    except Exception:
        return default


def compact_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def pretty_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, indent=2)


# =========================
# Data Models
# =========================

@dataclass
class Settings:
    concept: str = "healthy"
    default_servings: int = 2
    expiring_soon_days: int = 3
    daily_menu_count: int = 5
    auto_estimate_useby: str = "Y"
    max_estimate_per_run: int = 5
    overwrite_llm_estimates: str = "N"


@dataclass
class EstimateRequestItem:
    item: str
    storage: str
    purchased_date: str
    category: str
    notes: str


@dataclass
class EstimateResultItem:
    item: str
    estimated_use_by: str
    confidence: str
    basis: str


# =========================
# Google Sheets Client
# =========================

class SheetsClient:
    def __init__(self, sheet_id: str):
        self.sheet_id = sheet_id
        self.gc = self._authorize()
        self.sh = self.gc.open_by_key(sheet_id)

        # Ensure worksheets exist
        for name in REQUIRED_TABS:
            try:
                self.sh.worksheet(name)
            except Exception:
                die(f"Worksheet '{name}' not found. Create it first.")

        self.ws_inventory = self.sh.worksheet("Inventory")
        self.ws_pantry = self.sh.worksheet("Pantry")
        self.ws_settings = self.sh.worksheet("Settings")
        self.ws_dailymenus = self.sh.worksheet("DailyMenus")

    def _authorize(self) -> gspread.Client:
        sa_json = os.getenv("GOOGLE_SA_JSON", "")
        sa_b64 = os.getenv("GOOGLE_SA_JSON_B64", "")

        if sa_b64 and not sa_json:
            try:
                sa_json = base64.b64decode(sa_b64).decode("utf-8")
            except Exception as e:
                die(f"Failed to decode GOOGLE_SA_JSON_B64: {e}")

        if not sa_json:
            die("Missing GOOGLE_SA_JSON or GOOGLE_SA_JSON_B64 env var.")

        try:
            info = json.loads(sa_json)
        except Exception as e:
            die(f"GOOGLE_SA_JSON is not valid JSON: {e}")

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)

    @staticmethod
    def _read_header(ws: gspread.Worksheet) -> List[str]:
        header = ws.row_values(1)
        return [h.strip() for h in header]

    @staticmethod
    def _ensure_header(ws: gspread.Worksheet, expected: List[str]) -> None:
        header = SheetsClient._read_header(ws)
        if header != expected:
            # Fail fast (strict) because column name matching is critical.
            die(
                f"Header mismatch in '{ws.title}'.\n"
                f"Expected: {expected}\n"
                f"Actual  : {header}\n"
                f"Fix the sheet header to match exactly."
            )

    def validate_headers(self) -> None:
        self._ensure_header(self.ws_inventory, INVENTORY_COLUMNS)
        self._ensure_header(self.ws_pantry, PANTRY_COLUMNS)
        self._ensure_header(self.ws_settings, SETTINGS_COLUMNS)
        self._ensure_header(self.ws_dailymenus, DAILYMENUS_COLUMNS)

    def load_settings(self) -> Settings:
        # Settings uses only row 2 as the single config row
        self._ensure_header(self.ws_settings, SETTINGS_COLUMNS)
        row = self.ws_settings.row_values(2)
        # pad
        row += [""] * (len(SETTINGS_COLUMNS) - len(row))
        m = dict(zip(SETTINGS_COLUMNS, row))

        s = Settings(
            concept=(m.get("Concept") or "healthy").strip() or "healthy",
            default_servings=safe_int(m.get("DefaultServings"), 2),
            expiring_soon_days=safe_int(m.get("ExpiringSoonDays"), 3),
            daily_menu_count=safe_int(m.get("DailyMenuCount"), 5),
            auto_estimate_useby=normalize_yes_no(m.get("AutoEstimateUseBy"), "Y"),
            max_estimate_per_run=safe_int(m.get("MaxEstimatePerRun"), 5),
            overwrite_llm_estimates=normalize_yes_no(m.get("OverwriteLLMEstimates"), "N"),
        )
        return s

    def load_table(self, ws: gspread.Worksheet) -> List[Dict[str, str]]:
        header = self._read_header(ws)
        values = ws.get_all_values()
        if not values or len(values) == 1:
            return []
        rows = []
        for r in values[1:]:
            r += [""] * (len(header) - len(r))
            rows.append({header[i]: (r[i] if i < len(r) else "") for i in range(len(header))})
        return rows

    def load_inventory(self) -> List[Dict[str, str]]:
        self._ensure_header(self.ws_inventory, INVENTORY_COLUMNS)
        return self.load_table(self.ws_inventory)

    def load_pantry(self) -> List[Dict[str, str]]:
        self._ensure_header(self.ws_pantry, PANTRY_COLUMNS)
        return self.load_table(self.ws_pantry)

    def batch_update_cells(self, ws: gspread.Worksheet, updates: List[Tuple[int, str, Any]]) -> None:
        """
        updates: list of (row_index_1based, col_name, value)
        """
        if not updates:
            return
        header = self._read_header(ws)
        col_index = {name: i + 1 for i, name in enumerate(header)}
        cells = []
        for row_i, col_name, value in updates:
            if col_name not in col_index:
                die(f"Unknown column '{col_name}' in worksheet '{ws.title}'")
            c = gspread.Cell(row_i, col_index[col_name], str(value))
            cells.append(c)
        ws.update_cells(cells, value_input_option="USER_ENTERED")

    def append_daily_menus(
        self,
        date_str: str,
        mode: str,
        menus_json: str,
        alerts_json: str,
        requested_servings: str,
        concept_used: str,
        created_at: str,
        trigger: str,
    ) -> None:
        self._ensure_header(self.ws_dailymenus, DAILYMENUS_COLUMNS)
        row = [
            date_str,
            mode,
            menus_json,
            alerts_json,
            requested_servings,
            concept_used,
            created_at,
            trigger,
        ]
        self.ws_dailymenus.append_row(row, value_input_option="USER_ENTERED")

    def find_latest_menu_row_for_date(self, date_str: str) -> Optional[Dict[str, str]]:
        """
        Returns latest row dict for Date==date_str and Mode=='menu' (last occurrence).
        """
        self._ensure_header(self.ws_dailymenus, DAILYMENUS_COLUMNS)
        rows = self.load_table(self.ws_dailymenus)
        latest = None
        for r in rows:
            if (r.get("Date") or "").strip() == date_str and (r.get("Mode (menu/recipe/estimate)") or "").strip() == "menu":
                latest = r
        return latest


# =========================
# OpenAI (LLM) Client
# =========================

class LLMClient:
    def __init__(self, api_key: str, model: str = OPENAI_MODEL):
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def _chat_json(self, system: str, user: str, schema_hint: Optional[str] = None) -> Dict[str, Any]:
        """
        Uses response_format json_object for stable JSON.
        """
        messages = [
            {"role": "system", "content": system.strip()},
            {"role": "user", "content": user.strip()},
        ]
        # OpenAI Responses API is preferred, but Chat Completions is still used widely.
        # We'll use chat.completions with response_format if available.
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        content = resp.choices[0].message.content
        try:
            return json.loads(content)
        except Exception as e:
            raise ValueError(f"LLM returned non-JSON. error={e}, content={content[:500]}")

    def estimate_useby(self, items: List[EstimateRequestItem]) -> List[EstimateResultItem]:
        if not items:
            return []

        system = """
You estimate food "use by" dates.
Return ONLY valid JSON. No markdown.
Use conservative, practical home-cooking assumptions.
Confidence: high/medium/low.
"""
        # Minimal but explicit schema instruction
        user = {
            "task": "Estimate use-by dates",
            "input_items": [
                {
                    "item": it.item,
                    "storage": it.storage,
                    "purchasedDate": it.purchased_date,
                    "category": it.category,
                    "notes": it.notes,
                }
                for it in items
            ],
            "output_schema": {
                "estimates": [
                    {
                        "item": "string",
                        "estimatedUseBy": "YYYY-MM-DD",
                        "confidence": "high|medium|low",
                        "basis": "short explanation",
                    }
                ]
            },
            "rules": [
                "If purchasedDate is missing or invalid, make a reasonable estimate and set confidence=low.",
                "If storage is Freezer, typically longer than Fridge.",
                "If item is ambiguous, choose safe side (earlier date) and lower confidence.",
                "estimatedUseBy must be ISO date string.",
            ],
        }

        out = self._chat_json(system=system, user=compact_json(user))
        estimates = out.get("estimates", [])
        results: List[EstimateResultItem] = []
        for e in estimates:
            results.append(
                EstimateResultItem(
                    item=str(e.get("item", "")).strip(),
                    estimated_use_by=str(e.get("estimatedUseBy", "")).strip(),
                    confidence=str(e.get("confidence", "")).strip().lower(),
                    basis=str(e.get("basis", "")).strip(),
                )
            )
        return results

    def recommend_menus(
        self,
        concept: str,
        servings: int,
        menu_count: int,
        inventory_summary: List[Dict[str, Any]],
        alerts: Dict[str, Any],
    ) -> Dict[str, Any]:
        system = """
You are a meal planning assistant.
Return ONLY valid JSON that matches the schema.
No markdown. Use Korean food names if appropriate, but keep fields concise.
Nutrition is rough estimate per serving.
"""
        user = {
            "task": "Recommend menus for today",
            "concept": concept,
            "requestedServings": servings,
            "menuCount": menu_count,
            "alerts": alerts,
            "availableIngredients": inventory_summary,
            "schema": {
                "alerts": {"expired": [], "expiringSoon": []},
                "menus": [
                    {
                        "id": 1,
                        "name": "",
                        "cuisine": "",
                        "why": "",
                        "uses": [],
                        "missing": [],
                        "timeMin": 30,
                        "difficulty": "easy|medium|hard",
                        "nutritionPerServing": {
                            "kcal": 600,
                            "carb_g": 70,
                            "protein_g": 30,
                            "fat_g": 20,
                        },
                    }
                ],
            },
            "rules": [
                "Focus on using expiringSoon first; never suggest using expired items.",
                "Menus must be diverse; avoid duplicates.",
                "If missing ingredients are needed, list them in missing.",
                "Keep 'why' short and specific.",
                "IDs must be 1..menuCount",
            ],
        }

        out = self._chat_json(system=system, user=compact_json(user))
        return out

    def build_recipe_detail(
        self,
        menu_item: Dict[str, Any],
        servings: int,
        available_ingredients: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        system = """
You are a cooking assistant.
Return ONLY valid JSON.
No markdown.
"""
        user = {
            "task": "Create a detailed recipe",
            "menu": menu_item,
            "servings": servings,
            "availableIngredients": available_ingredients,
            "schema": {
                "name": "string",
                "servings": 2,
                "ingredients": [{"name": "string", "amount": "string", "optional": False}],
                "steps": ["string"],
                "tips": ["string"],
                "substitutions": [{"from": "string", "to": "string"}],
                "timeMin": 30,
            },
            "rules": [
                "Prefer using available ingredients; if needed, mention optional items.",
                "Steps should be short, numbered style text is okay inside strings.",
            ],
        }

        out = self._chat_json(system=system, user=compact_json(user))
        return out


# =========================
# Domain Logic
# =========================

def build_inventory_summary(inventory: List[Dict[str, str]], pantry: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """
    Reduce raw rows into compact ingredient objects for LLM.
    """
    out: List[Dict[str, Any]] = []

    def add_row(src: str, r: Dict[str, str]) -> None:
        item = (r.get("Item") or "").strip()
        if not item:
            return
        qty = (r.get("Qty") or "").strip()
        unit = (r.get("Unit") or "").strip()
        useby = (r.get("UseBy_Est (YYYY-MM-DD)") or r.get("UseBy_Est") or "").strip()
        storage = (r.get("Storage (Fridge/Freezer/Pantry)") or src).strip()
        cat = (r.get("Category") or "").strip()
        notes = (r.get("Notes") or "").strip()
        out.append(
            {
                "item": item,
                "qty": qty,
                "unit": unit,
                "storage": storage,
                "useBy": useby,
                "category": cat,
                "notes": notes,
                "source": src,
            }
        )

    for r in inventory:
        add_row("Inventory", r)
    for r in pantry:
        add_row("Pantry", r)

    return out


def compute_alerts(
    inventory: List[Dict[str, str]],
    pantry: List[Dict[str, str]],
    expiring_soon_days: int,
    today: dt.date,
) -> Dict[str, Any]:
    expired = []
    expiring_soon = []

    def check_row(src: str, r: Dict[str, str]) -> None:
        item = (r.get("Item") or "").strip()
        if not item:
            return

        useby_str = (r.get("UseBy_Est (YYYY-MM-DD)") or r.get("UseBy_Est") or "").strip()
        d = parse_yyyy_mm_dd(useby_str)
        if not d:
            return

        entry = {
            "item": item,
            "useBy": useby_str,
            "source": src,
        }
        if d < today:
            expired.append(entry)
        elif today <= d <= (today + dt.timedelta(days=expiring_soon_days)):
            expiring_soon.append(entry)

    for r in inventory:
        check_row("Inventory", r)
    for r in pantry:
        check_row("Pantry", r)

    # sort by date
    expired.sort(key=lambda x: x["useBy"])
    expiring_soon.sort(key=lambda x: x["useBy"])

    return {"expired": expired, "expiringSoon": expiring_soon}


def select_useby_estimation_targets(
    settings: Settings,
    inventory: List[Dict[str, str]],
    pantry: List[Dict[str, str]],
) -> Tuple[List[Tuple[str, int, Dict[str, str]]], List[EstimateRequestItem]]:
    """
    Returns:
      - targets: list of (tab_name, row_index_1based, row_dict)
      - llm_items: list of EstimateRequestItem (same order)
    """
    targets: List[Tuple[str, int, Dict[str, str]]] = []
    llm_items: List[EstimateRequestItem] = []

    if settings.auto_estimate_useby != "Y":
        return targets, llm_items

    def consider(tab: str, rows: List[Dict[str, str]], header: List[str]) -> None:
        # Row index: +2 because row 1 is header, rows list starts at sheet row 2.
        for i, r in enumerate(rows, start=2):
            if len(targets) >= settings.max_estimate_per_run:
                return

            item = (r.get("Item") or "").strip()
            if not item:
                continue

            # column naming differs between Inventory and Pantry
            useby_key = "UseBy_Est (YYYY-MM-DD)" if tab == "Inventory" else "UseBy_Est"
            source_key = "UseBy_Source (llm/user)" if tab == "Inventory" else "UseBy_Source"

            useby = (r.get(useby_key) or "").strip()
            src = (r.get(source_key) or "").strip().lower()

            # If empty -> eligible
            if not useby:
                targets.append((tab, i, r))
                llm_items.append(
                    EstimateRequestItem(
                        item=item,
                        storage=(r.get("Storage (Fridge/Freezer/Pantry)") or "").strip() if tab == "Inventory" else "Pantry",
                        purchased_date=(r.get("PurchasedDate (YYYY-MM-DD)") or "").strip() if tab == "Inventory" else "",
                        category=(r.get("Category") or "").strip() if tab == "Inventory" else "",
                        notes=(r.get("Notes") or "").strip(),
                    )
                )
                continue

            # If has value:
            # user edited -> never touch
            if src == "user":
                continue

            # llm edited -> overwrite only if enabled
            if src == "llm" and settings.overwrite_llm_estimates == "Y":
                targets.append((tab, i, r))
                llm_items.append(
                    EstimateRequestItem(
                        item=item,
                        storage=(r.get("Storage (Fridge/Freezer/Pantry)") or "").strip() if tab == "Inventory" else "Pantry",
                        purchased_date=(r.get("PurchasedDate (YYYY-MM-DD)") or "").strip() if tab == "Inventory" else "",
                        category=(r.get("Category") or "").strip() if tab == "Inventory" else "",
                        notes=(r.get("Notes") or "").strip(),
                    )
                )

    consider("Inventory", inventory, INVENTORY_COLUMNS)
    consider("Pantry", pantry, PANTRY_COLUMNS)

    return targets, llm_items


def apply_useby_estimates_to_sheets(
    sheets: SheetsClient,
    targets: List[Tuple[str, int, Dict[str, str]]],
    estimates: List[EstimateResultItem],
) -> Dict[str, Any]:
    """
    Updates corresponding rows in Inventory/Pantry.
    Returns summary dict.
    """
    if not targets:
        return {"updated": 0, "skipped": 0, "details": []}

    # Map item->estimate (LLM should return same items; still handle mismatches)
    est_map = {}
    for e in estimates:
        if e.item:
            est_map[e.item] = e

    inv_updates: List[Tuple[int, str, Any]] = []
    pan_updates: List[Tuple[int, str, Any]] = []
    details = []
    updated = 0
    skipped = 0

    for (tab, row_i, r) in targets:
        item = (r.get("Item") or "").strip()
        e = est_map.get(item)
        if not e or not parse_yyyy_mm_dd(e.estimated_use_by):
            skipped += 1
            details.append({"item": item, "status": "skipped", "reason": "missing_or_invalid_llm_estimate"})
            continue

        if tab == "Inventory":
            inv_updates.extend(
                [
                    (row_i, "UseBy_Est (YYYY-MM-DD)", e.estimated_use_by),
                    (row_i, "UseBy_Source (llm/user)", "llm"),
                    (row_i, "UseBy_Confidence (high/medium/low)", e.confidence),
                    (row_i, "UseBy_Basis", e.basis),
                ]
            )
        else:
            pan_updates.extend(
                [
                    (row_i, "UseBy_Est", e.estimated_use_by),
                    (row_i, "UseBy_Source", "llm"),
                    (row_i, "UseBy_Confidence", e.confidence),
                    (row_i, "UseBy_Basis", e.basis),
                ]
            )

        updated += 1
        details.append(
            {
                "item": item,
                "status": "updated",
                "useBy": e.estimated_use_by,
                "confidence": e.confidence,
            }
        )

    if inv_updates:
        sheets.batch_update_cells(sheets.ws_inventory, inv_updates)
    if pan_updates:
        sheets.batch_update_cells(sheets.ws_pantry, pan_updates)

    return {"updated": updated, "skipped": skipped, "details": details}


# =========================
# Telegram
# =========================

def telegram_send_message(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        # Silent no-op (useful in CI without telegram)
        print("[INFO] Telegram env vars missing; skip sending.")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",  # keep formatting mild
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")

    req = urllib.request.Request(url, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = resp.read().decode("utf-8", errors="ignore")
        if resp.status >= 400:
            raise RuntimeError(f"Telegram API error: {resp.status} {body}")


def format_menu_message(date_str: str, concept: str, servings: int, alerts: Dict[str, Any], menus_obj: Dict[str, Any]) -> str:
    expired = alerts.get("expired", [])
    soon = alerts.get("expiringSoon", [])

    lines = []
    lines.append(f"<b>🍽 오늘의 메뉴 추천</b> ({date_str})")
    lines.append(f"컨셉: <b>{concept}</b> / 인분: <b>{servings}</b>")
    lines.append("")

    if expired:
        lines.append("<b>⛔ 만료(사용 금지)</b>")
        for x in expired[:10]:
            lines.append(f"- {x.get('item','')} ({x.get('useBy','')})")
        lines.append("")

    if soon:
        lines.append("<b>⚠️ 임박(우선 사용)</b>")
        for x in soon[:10]:
            lines.append(f"- {x.get('item','')} ({x.get('useBy','')})")
        lines.append("")

    menus = menus_obj.get("menus", [])
    lines.append("<b>추천 메뉴</b>")
    for m in menus:
        mid = m.get("id")
        name = m.get("name", "")
        why = m.get("why", "")
        time_min = m.get("timeMin", "")
        diff = m.get("difficulty", "")
        uses = m.get("uses", [])
        missing = m.get("missing", [])
        lines.append(f"\n<b>{mid}. {name}</b>  ({diff}, {time_min}min)")
        if why:
            lines.append(f"- 이유: {why}")
        if uses:
            lines.append(f"- 사용: {', '.join(uses[:10])}")
        if missing:
            lines.append(f"- 추가 필요: {', '.join(missing[:10])}")

    lines.append("\n<code>/recipe N</code> 으로 상세 레시피 요청")
    return "\n".join(lines)


def format_estimate_message(date_str: str, summary: Dict[str, Any]) -> str:
    updated = summary.get("updated", 0)
    skipped = summary.get("skipped", 0)
    lines = []
    lines.append(f"<b>📅 소비기한 추정 결과</b> ({date_str})")
    lines.append(f"- 업데이트: <b>{updated}</b> / 스킵: <b>{skipped}</b>")
    details = summary.get("details", [])
    for d in details[:15]:
        if d.get("status") == "updated":
            lines.append(f"  - {d.get('item')} → {d.get('useBy')} ({d.get('confidence')})")
        else:
            lines.append(f"  - {d.get('item')} (스킵: {d.get('reason')})")
    return "\n".join(lines)


def format_recipe_message(recipe_obj: Dict[str, Any]) -> str:
    name = recipe_obj.get("name", "")
    servings = recipe_obj.get("servings", "")
    time_min = recipe_obj.get("timeMin", "")
    ingredients = recipe_obj.get("ingredients", [])
    steps = recipe_obj.get("steps", [])
    tips = recipe_obj.get("tips", [])

    lines = []
    lines.append(f"<b>👩‍🍳 레시피</b> - <b>{name}</b>")
    lines.append(f"인분: <b>{servings}</b> / 예상: <b>{time_min}min</b>")
    lines.append("\n<b>재료</b>")
    for ing in ingredients[:30]:
        nm = ing.get("name", "")
        amt = ing.get("amount", "")
        opt = ing.get("optional", False)
        tag = " (옵션)" if opt else ""
        lines.append(f"- {nm}: {amt}{tag}")

    lines.append("\n<b>조리 순서</b>")
    for i, s in enumerate(steps[:25], start=1):
        lines.append(f"{i}. {s}")

    if tips:
        lines.append("\n<b>팁</b>")
        for t in tips[:10]:
            lines.append(f"- {t}")

    return "\n".join(lines)


# =========================
# Modes
# =========================

def run_estimate_mode(sheets: SheetsClient, llm: LLMClient, settings: Settings, trigger: str) -> Dict[str, Any]:
    inventory = sheets.load_inventory()
    pantry = sheets.load_pantry()

    targets, req_items = select_useby_estimation_targets(settings, inventory, pantry)
    if not req_items:
        summary = {"updated": 0, "skipped": 0, "details": []}
        # Save to DailyMenus (optional)
        sheets.append_daily_menus(
            date_str=today_yyyy_mm_dd_kst(),
            mode="estimate",
            menus_json="",
            alerts_json="",
            requested_servings="",
            concept_used=settings.concept,
            created_at=utcnow_iso(),
            trigger=trigger,
        )
        return summary

    estimates = llm.estimate_useby(req_items)
    summary = apply_useby_estimates_to_sheets(sheets, targets, estimates)

    sheets.append_daily_menus(
        date_str=today_yyyy_mm_dd_kst(),
        mode="estimate",
        menus_json="",
        alerts_json="",
        requested_servings="",
        concept_used=settings.concept,
        created_at=utcnow_iso(),
        trigger=trigger,
    )
    return summary


def run_menu_mode(
    sheets: SheetsClient,
    llm: LLMClient,
    settings: Settings,
    trigger: str,
    servings_override: Optional[int],
    concept_override: Optional[str],
    use_cache: bool = True,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[Dict[str, Any]]]:
    date_str = today_yyyy_mm_dd_kst()
    concept = (concept_override or settings.concept).strip() or settings.concept
    servings = servings_override or settings.default_servings

    # Cache: if today's menu exists and trigger is schedule, reuse (optional)
    if use_cache and trigger == "schedule":
        cached = sheets.find_latest_menu_row_for_date(date_str)
        if cached and (cached.get("MenusJSON") or "").strip():
            try:
                menus_obj = json.loads(cached["MenusJSON"])
                alerts_obj = json.loads(cached.get("AlertsJSON") or "{}")
                inventory = sheets.load_inventory()
                pantry = sheets.load_pantry()
                inv_summary = build_inventory_summary(inventory, pantry)
                return menus_obj, alerts_obj, inv_summary
            except Exception:
                # Fall through to regenerate
                pass

    # Step 1: load
    inventory = sheets.load_inventory()
    pantry = sheets.load_pantry()

    # Step 2: estimate (if enabled)
    if settings.auto_estimate_useby == "Y":
        targets, req_items = select_useby_estimation_targets(settings, inventory, pantry)
        if req_items:
            estimates = llm.estimate_useby(req_items)
            _ = apply_useby_estimates_to_sheets(sheets, targets, estimates)
            # reload after updates for alerts/menu correctness
            inventory = sheets.load_inventory()
            pantry = sheets.load_pantry()

    # Step 3: alerts calculation
    today = parse_yyyy_mm_dd(date_str) or dt.date.today()
    alerts = compute_alerts(inventory, pantry, settings.expiring_soon_days, today)

    # Step 4: menu recommend
    inv_summary = build_inventory_summary(inventory, pantry)
    menus_obj = llm.recommend_menus(
        concept=concept,
        servings=servings,
        menu_count=settings.daily_menu_count,
        inventory_summary=inv_summary,
        alerts=alerts,
    )

    # Step 5: save DailyMenus
    sheets.append_daily_menus(
        date_str=date_str,
        mode="menu",
        menus_json=compact_json(menus_obj),
        alerts_json=compact_json(alerts),
        requested_servings=str(servings),
        concept_used=concept,
        created_at=utcnow_iso(),
        trigger=trigger,
    )

    return menus_obj, alerts, inv_summary


def run_recipe_mode(
    sheets: SheetsClient,
    llm: LLMClient,
    settings: Settings,
    trigger: str,
    recipe_id: int,
    servings_override: Optional[int],
) -> Dict[str, Any]:
    date_str = today_yyyy_mm_dd_kst()
    servings = servings_override or settings.default_servings

    latest = sheets.find_latest_menu_row_for_date(date_str)
    if not latest or not (latest.get("MenusJSON") or "").strip():
        # fallback: find any latest menu (scan whole table)
        rows = sheets.load_table(sheets.ws_dailymenus)
        latest_any = None
        for r in rows:
            if (r.get("Mode (menu/recipe/estimate)") or "").strip() == "menu" and (r.get("MenusJSON") or "").strip():
                latest_any = r
        latest = latest_any

    if not latest:
        die("No menu found in DailyMenus. Run menu mode first.")

    menus_obj = json.loads(latest["MenusJSON"])
    menus = menus_obj.get("menus", [])
    target = None
    for m in menus:
        try:
            if int(m.get("id")) == int(recipe_id):
                target = m
                break
        except Exception:
            continue

    if not target:
        die(f"Recipe id {recipe_id} not found in latest menus.")

    inventory = sheets.load_inventory()
    pantry = sheets.load_pantry()
    inv_summary = build_inventory_summary(inventory, pantry)

    recipe_obj = llm.build_recipe_detail(menu_item=target, servings=servings, available_ingredients=inv_summary)

    # Optional: store in DailyMenus as mode=recipe (recommended for traceability)
    sheets.append_daily_menus(
        date_str=today_yyyy_mm_dd_kst(),
        mode="recipe",
        menus_json=compact_json(recipe_obj),
        alerts_json="",
        requested_servings=str(servings),
        concept_used=(latest.get("ConceptUsed") or settings.concept),
        created_at=utcnow_iso(),
        trigger=trigger,
    )

    return recipe_obj


# =========================
# CLI
# =========================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fridge sync: estimate use-by, recommend menus, send Telegram, cache to Google Sheets.")
    p.add_argument("--mode", required=True, choices=["menu", "estimate", "recipe"], help="Run mode")
    p.add_argument("--recipe_id", type=int, default=None, help="Recipe ID (required for recipe mode)")
    p.add_argument("--servings", type=int, default=None, help="Override servings")
    p.add_argument("--concept", type=str, default=None, help="Override concept")
    p.add_argument("--trigger", type=str, default="schedule", choices=["schedule", "telegram"], help="Trigger source")
    p.add_argument("--no_cache", action="store_true", help="Disable cache (schedule trigger default uses cache)")
    p.add_argument("--dry_run", action="store_true", help="Do not send telegram")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()
    if not sheet_id:
        die("Missing GOOGLE_SHEET_ID env var.")
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        die("Missing OPENAI_API_KEY env var.")

    sheets = SheetsClient(sheet_id=sheet_id)
    sheets.validate_headers()
    settings = sheets.load_settings()

    llm = LLMClient(api_key=api_key, model=OPENAI_MODEL)

    date_str = today_yyyy_mm_dd_kst()

    if args.mode == "estimate":
        summary = run_estimate_mode(sheets, llm, settings, trigger=args.trigger)
        msg = format_estimate_message(date_str, summary)
        print(msg)
        if not args.dry_run:
            telegram_send_message(msg)
        return

    if args.mode == "menu":
        menus_obj, alerts, _inv_summary = run_menu_mode(
            sheets=sheets,
            llm=llm,
            settings=settings,
            trigger=args.trigger,
            servings_override=args.servings,
            concept_override=args.concept,
            use_cache=(not args.no_cache),
        )
        concept_used = (args.concept or settings.concept).strip() or settings.concept
        servings_used = args.servings or settings.default_servings
        msg = format_menu_message(date_str, concept_used, servings_used, alerts, menus_obj)
        print(msg)
        if not args.dry_run:
            telegram_send_message(msg)
        return

    if args.mode == "recipe":
        if args.recipe_id is None:
            die("--recipe_id is required for recipe mode.")
        recipe_obj = run_recipe_mode(
            sheets=sheets,
            llm=llm,
            settings=settings,
            trigger=args.trigger,
            recipe_id=args.recipe_id,
            servings_override=args.servings,
        )
        msg = format_recipe_message(recipe_obj)
        print(msg)
        if not args.dry_run:
            telegram_send_message(msg)
        return


if __name__ == "__main__":
    main()
