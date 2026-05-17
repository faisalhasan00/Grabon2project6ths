"""
Visual intelligence — OCR, hero banners, campaign detection, image offers.

Many coupon-site campaigns exist only in banners/hero images. This module
turns screenshots from passive evidence into structured market signals.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

VISION_MODEL = os.getenv("CRAWLER_VISION_MODEL", "gemini-2.0-flash")
PHASH_MAX_DISTANCE = int(os.getenv("CRAWLER_PHASH_MAX_DISTANCE", "14"))

SALE_KEYWORDS = (
    "sale", "billion", "festive", "diwali", "black friday", "big billion",
    "end of season", "clearance", "mega", "bonanza", "flash", "limited",
)

CAMPAIGN_MARKERS = (
    "days", "fest", "live now", "starts", "ends", "today only", "hour",
)


@dataclass
class ScreenshotComparison:
    previous_phash: Optional[str]
    current_phash: str
    hamming_distance: Optional[int]
    visually_changed: bool
    hero_changed: bool = False
    hero_hamming: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "previous_phash": self.previous_phash,
            "current_phash": self.current_phash,
            "hamming_distance": self.hamming_distance,
            "visually_changed": self.visually_changed,
            "hero_changed": self.hero_changed,
            "hero_hamming": self.hero_hamming,
        }


@dataclass
class VisualIntelResult:
    offers: List[str] = field(default_factory=list)
    campaigns: List[str] = field(default_factory=list)
    hero_headline: Optional[str] = None
    ocr_text: str = ""
    cashback_rate: Optional[str] = None
    sale_detected: bool = False
    campaign_detected: bool = False
    perceptual_hash: str = ""
    hero_perceptual_hash: str = ""
    comparison: Optional[ScreenshotComparison] = None
    method: str = "none"
    cost_usd: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "offers": self.offers,
            "campaigns": self.campaigns,
            "hero_headline": self.hero_headline,
            "ocr_text": self.ocr_text[:2000],
            "cashback_rate": self.cashback_rate,
            "sale_detected": self.sale_detected,
            "campaign_detected": self.campaign_detected,
            "perceptual_hash": self.perceptual_hash,
            "hero_perceptual_hash": self.hero_perceptual_hash,
            "comparison": self.comparison.to_dict() if self.comparison else None,
            "method": self.method,
        }


def visual_mode() -> str:
    """always | sparse | off"""
    return os.getenv("CRAWLER_VISUAL_MODE", "always").lower()


def vision_enabled() -> bool:
    if visual_mode() == "off":
        return False
    if os.getenv("CRAWLER_VISION_ENABLED", "true").lower() == "false":
        return False
    return bool(os.getenv("GOOGLE_API_KEY"))


def should_run_visual(*, dom_offer_count: int) -> bool:
    mode = visual_mode()
    if mode == "off":
        return False
    if mode == "always":
        return True
    return dom_offer_count < 3


def perceptual_hash(image_path: str, *, size: int = 9) -> str:
    """Difference hash (dHash) as hex — for screenshot comparison."""
    try:
        from PIL import Image
    except ImportError:
        return ""

    img = Image.open(image_path).convert("L").resize((size, size - 1))
    pixels = list(img.getdata())
    bits = []
    for row in range(size - 1):
        for col in range(size):
            left = pixels[row * size + col]
            right = pixels[row * size + col + 1]
            bits.append(1 if left > right else 0)
    value = int("".join(str(b) for b in bits), 2)
    return f"{value:016x}"


def hamming_distance_hex(h1: str, h2: str) -> Optional[int]:
    if not h1 or not h2 or len(h1) != len(h2):
        return None
    try:
        a, b = int(h1, 16), int(h2, 16)
        return (a ^ b).bit_count()
    except ValueError:
        return None


def compare_screenshots(
    current_phash: str,
    previous_phash: Optional[str],
    *,
    current_hero_phash: str = "",
    previous_hero_phash: Optional[str] = None,
) -> ScreenshotComparison:
    dist = hamming_distance_hex(current_phash, previous_phash) if previous_phash else None
    hero_dist = (
        hamming_distance_hex(current_hero_phash, previous_hero_phash)
        if previous_hero_phash and current_hero_phash
        else None
    )
    changed = dist is None or dist > PHASH_MAX_DISTANCE
    hero_changed = hero_dist is not None and hero_dist > max(8, PHASH_MAX_DISTANCE // 2)
    return ScreenshotComparison(
        previous_phash=previous_phash,
        current_phash=current_phash,
        hamming_distance=dist,
        visually_changed=changed,
        hero_changed=hero_changed,
        hero_hamming=hero_dist,
    )


def crop_hero_banner(full_path: str, *, height_ratio: float = 0.38) -> Optional[str]:
    """Extract top hero region where campaigns usually live."""
    try:
        from PIL import Image
    except ImportError:
        return None
    if not os.path.isfile(full_path):
        return None
    base, ext = os.path.splitext(full_path)
    hero_path = f"{base}_hero{ext}"
    try:
        img = Image.open(full_path)
        w, h = img.size
        crop_h = max(120, int(h * height_ratio))
        hero = img.crop((0, 0, w, min(crop_h, h)))
        hero.save(hero_path)
        return hero_path
    except OSError:
        return None


def ocr_local(image_path: str) -> str:
    """Optional Tesseract OCR (no-op if unavailable)."""
    if os.getenv("CRAWLER_TESSERACT_ENABLED", "false").lower() != "true":
        return ""
    try:
        import pytesseract
        from PIL import Image
    except ImportError:
        return ""
    try:
        text = pytesseract.image_to_string(Image.open(image_path))
        return re.sub(r"\s+", " ", (text or "").strip())
    except Exception:
        return ""


def _detect_sale_from_text(*parts: str) -> bool:
    blob = " ".join(p for p in parts if p).lower()
    return any(k in blob for k in SALE_KEYWORDS)


def _detect_campaign_from_text(*parts: str) -> bool:
    blob = " ".join(p for p in parts if p).lower()
    has_sale = _detect_sale_from_text(blob)
    has_campaign = any(m in blob for m in CAMPAIGN_MARKERS)
    return has_sale or (has_campaign and "%" in blob)


def _parse_vision_json(text: str) -> Dict[str, Any]:
    try:
        m = re.search(r"\{.*\}", text, re.DOTALL)
        if m:
            return json.loads(m.group(0))
    except json.JSONDecodeError:
        pass
    return {}


async def analyze_screenshot(
    screenshot_path: str,
    *,
    merchant: str,
    source_name: str,
    hero_path: Optional[str] = None,
    previous_phash: Optional[str] = None,
    previous_hero_phash: Optional[str] = None,
) -> Tuple[VisualIntelResult, float]:
    """
    Full visual intelligence pass: phash, OCR, hero analysis, offer extraction.
    """
    result = VisualIntelResult()
    if not screenshot_path or not os.path.isfile(screenshot_path):
        return result, 0.0

    result.perceptual_hash = perceptual_hash(screenshot_path)
    if hero_path and os.path.isfile(hero_path):
        result.hero_perceptual_hash = perceptual_hash(hero_path)
    else:
        hero_path = crop_hero_banner(screenshot_path)
        if hero_path:
            result.hero_perceptual_hash = perceptual_hash(hero_path)

    result.comparison = compare_screenshots(
        result.perceptual_hash,
        previous_phash,
        current_hero_phash=result.hero_perceptual_hash,
        previous_hero_phash=previous_hero_phash,
    )

    ocr_parts = [ocr_local(screenshot_path)]
    if hero_path:
        ocr_parts.append(ocr_local(hero_path))
    result.ocr_text = " | ".join(p for p in ocr_parts if p)

    if vision_enabled():
        gemini_data, cost = await _gemini_visual_analysis(
            screenshot_path,
            hero_path=hero_path,
            merchant=merchant,
            source_name=source_name,
        )
        result.cost_usd = cost
        result.method = "gemini+phash" if result.ocr_text else "gemini+phash+ocr"
        _apply_gemini_result(result, gemini_data)
    elif result.ocr_text:
        result.method = "ocr+phash"
        _apply_ocr_fallback(result)

    if not result.campaign_detected:
        result.campaign_detected = _detect_campaign_from_text(
            result.ocr_text, result.hero_headline or "", " ".join(result.campaigns)
        )
    if not result.sale_detected:
        result.sale_detected = _detect_sale_from_text(
            result.ocr_text, result.hero_headline or "", " ".join(result.campaigns)
        )

    return result, result.cost_usd


def _apply_gemini_result(result: VisualIntelResult, data: Dict[str, Any]) -> None:
    result.hero_headline = data.get("hero_headline") or data.get("hero_banner_text")
    campaigns = list(data.get("campaigns") or [])
    if data.get("campaign_name") and data["campaign_name"] not in campaigns:
        campaigns.insert(0, str(data["campaign_name"]))
    result.campaigns = [str(c) for c in campaigns[:8]]

    offers = [str(o) for o in (data.get("offers") or [])[:15]]
    result.offers = [_tag_visual(o) for o in offers if o]

    banner = data.get("banner_text") or data.get("ocr_text") or ""
    if banner and not result.ocr_text:
        result.ocr_text = str(banner)[:2000]
    elif banner:
        result.ocr_text = f"{result.ocr_text} | {banner}"[:2000]

    pct = data.get("cashback_percent") or data.get("cashback_pct")
    if pct is not None:
        try:
            result.cashback_rate = f"{float(pct):g}%"
        except (TypeError, ValueError):
            pass
    elif data.get("cashback_rate"):
        result.cashback_rate = str(data["cashback_rate"])

    result.sale_detected = bool(data.get("sale_detected")) or _detect_sale_from_text(
        result.ocr_text, result.hero_headline or "", " ".join(result.campaigns)
    )
    result.campaign_detected = bool(data.get("campaign_detected")) or _detect_campaign_from_text(
        result.ocr_text, result.hero_headline or "", " ".join(result.campaigns)
    )


def _apply_ocr_fallback(result: VisualIntelResult) -> None:
    text = result.ocr_text
    rates = re.findall(r"(\d+(?:\.\d+)?)\s*%\s*(?:cash\s*back|cashback)?", text, re.I)
    if rates and not result.cashback_rate:
        result.cashback_rate = f"{max(float(r) for r in rates):g}%"
    lines = [ln.strip() for ln in re.split(r"[\n|]", text) if len(ln.strip()) > 6]
    for ln in lines[:10]:
        if any(k in ln.lower() for k in ("cashback", "coupon", "sale", "%", "off")):
            result.offers.append(_tag_visual(ln[:160]))
    result.sale_detected = _detect_sale_from_text(text)
    result.campaign_detected = _detect_campaign_from_text(text)


def _tag_visual(text: str) -> str:
    t = text.strip()
    if t.lower().startswith("[visual]"):
        return t
    return f"[Visual] {t}"


async def _gemini_visual_analysis(
    screenshot_path: str,
    *,
    hero_path: Optional[str],
    merchant: str,
    source_name: str,
) -> Tuple[Dict[str, Any], float]:
    try:
        import google.generativeai as genai
        from PIL import Image
    except ImportError:
        return {}, 0.0

    genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
    model = genai.GenerativeModel(VISION_MODEL)

    prompt = f"""
You are analyzing coupon/cashback site screenshots for merchant "{merchant}" on {source_name}.

Tasks:
1. HERO BANNER: Read the main top banner/hero — headline, sale name, dates.
2. OCR: Transcribe ALL promotional text visible in images and banners (not nav/footer).
3. CAMPAIGNS: Name active sales (e.g. "Big Billion Days", "End of Season Sale").
4. OFFERS: List cashback %, coupon codes, bank offers, exclusives seen ONLY in images.
5. FLAGS: sale_detected if any sale/festival; campaign_detected if a named campaign runs.

Return ONLY valid JSON:
{{
  "hero_headline": "string or null",
  "hero_banner_text": "string or null",
  "campaign_name": "string or null",
  "campaigns": ["string"],
  "offers": ["plain text offer lines"],
  "cashback_percent": number or null,
  "banner_text": "full promotional OCR from banners",
  "sale_detected": true/false,
  "campaign_detected": true/false
}}
If captcha/block page with no promos, return empty offers and false flags.
"""

    images = [Image.open(screenshot_path)]
    if hero_path and os.path.isfile(hero_path):
        images.append(Image.open(hero_path))

    def _call():
        return model.generate_content([prompt] + images).text or ""

    try:
        text = await asyncio.to_thread(_call)
    except Exception as e:
        print(f"      [Visual] Gemini failed {source_name}: {e}")
        return {}, 0.0

    return _parse_vision_json(text), 0.0002 * len(images)


async def extract_visual_offers(
    screenshot_path: str,
    *,
    merchant: str,
    source_name: str,
    **kwargs: Any,
) -> Tuple[List[str], float]:
    """Backward-compatible: offers list + cost."""
    result, cost = await analyze_screenshot(
        screenshot_path,
        merchant=merchant,
        source_name=source_name,
        previous_phash=kwargs.get("previous_phash"),
        previous_hero_phash=kwargs.get("previous_hero_phash"),
        hero_path=kwargs.get("hero_path"),
    )
    return result.offers, cost
