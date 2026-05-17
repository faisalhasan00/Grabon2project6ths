"""Immutable evidence: full-page + hero screenshots, hashes, perceptual fingerprints."""
from __future__ import annotations

import hashlib
import os
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from agents.crawler.crawl.visual_intelligence import crop_hero_banner, perceptual_hash

EVIDENCE_DIR = "evidence"


@dataclass
class EvidenceRecord:
    path: str
    sha256: str
    timestamp: int
    url: str
    hero_path: Optional[str] = None
    hero_sha256: Optional[str] = None
    perceptual_hash: Optional[str] = None
    hero_perceptual_hash: Optional[str] = None

    def to_dict(self) -> dict:
        out = {
            "screenshot": self.path,
            "screenshot_hash": f"sha256:{self.sha256}",
            "timestamp": self.timestamp,
            "url_visited": self.url,
        }
        if self.hero_path:
            out["hero_screenshot"] = self.hero_path
        if self.hero_sha256:
            out["hero_screenshot_hash"] = f"sha256:{self.hero_sha256}"
        if self.perceptual_hash:
            out["perceptual_hash"] = self.perceptual_hash
        if self.hero_perceptual_hash:
            out["hero_perceptual_hash"] = self.hero_perceptual_hash
        return out


def file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


async def save_screenshot(page, target_name: str, url: str) -> Optional[EvidenceRecord]:
    """Full-page screenshot + hero crop + perceptual hashes."""
    os.makedirs(EVIDENCE_DIR, exist_ok=True)
    ts = int(time.time())
    safe_name = target_name.replace(" ", "_")
    path = os.path.join(EVIDENCE_DIR, f"{safe_name}_{ts}_{uuid.uuid4().hex[:8]}.png")
    try:
        await page.screenshot(path=path, full_page=False)
        if not os.path.isfile(path):
            return None

        digest = file_sha256(path)
        phash = perceptual_hash(path)
        hero_path = crop_hero_banner(path)
        hero_digest = file_sha256(hero_path) if hero_path else None
        hero_phash = perceptual_hash(hero_path) if hero_path else None

        return EvidenceRecord(
            path=path,
            sha256=digest,
            timestamp=ts,
            url=url,
            hero_path=hero_path,
            hero_sha256=hero_digest,
            perceptual_hash=phash,
            hero_perceptual_hash=hero_phash,
        )
    except Exception:
        return None
