from __future__ import annotations
import re
import unicodedata
from difflib import SequenceMatcher
from rapidfuzz import fuzz
from ingestion.districts_canonical import CANONICAL_CROP_DISTRICTS
_WORD = re.compile('[A-Za-z]+')

def _ascii_fold(s: str) -> str:
    s = unicodedata.normalize('NFKD', s)
    return s.encode('ascii', 'ignore').decode('ascii')

def alnum_compact(s: str) -> str:
    s = _ascii_fold(s.strip().lower())
    return re.sub('[^a-z0-9]+', '', s)

def space_fold(s: str) -> str:
    t = _ascii_fold(s.strip().lower())
    t = re.sub('[.\\s_]+', ' ', t)
    t = re.sub('[^a-z0-9 ]+', ' ', t)
    return re.sub('\\s+', ' ', t).strip()

def _word_acronym(canonical: str) -> str | None:
    words = _WORD.findall(canonical)
    if len(words) < 2:
        return None
    ac = ''.join((w[0] for w in words)).lower()
    return ac if len(ac) >= 2 else None

def _ratio(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

def _words_lower(canonical: str) -> list[str]:
    return [w.lower() for w in _WORD.findall(canonical)]

def _match_dot_initials_remainder(text: str) -> str | None:
    parts = [p.strip().lower() for p in text.split('.') if p.strip()]
    if len(parts) < 2:
        return None
    initials: list[str] = []
    i = 0
    while i < len(parts) and len(parts[i]) == 1:
        initials.append(parts[i])
        i += 1
    if not initials or i >= len(parts):
        return None
    remainder = ''.join(parts[i:])
    if not remainder:
        return None
    for name in CANONICAL_CROP_DISTRICTS:
        words = _words_lower(name)
        if len(words) < len(initials):
            continue
        if any((words[j][0] != initials[j] for j in range(len(initials)))):
            continue
        if len(words) > len(initials):
            rest = ''.join(words[len(initials):])
            if remainder == rest or rest.startswith(remainder) or remainder in rest:
                return name
            continue
        last = words[-1]
        if last.endswith(remainder) or remainder in last:
            return name
    return None

def _fuzzy_against_canonical(raw_c: str, raw_spaced: str, canonical: str, *, short_token: bool) -> float:
    cc = alnum_compact(canonical)
    parts = [_ratio(raw_c, cc), fuzz.ratio(raw_c, cc) / 100.0]
    if not short_token:
        parts.extend([fuzz.partial_ratio(raw_c, cc) / 100.0, fuzz.WRatio(raw_c, canonical) / 100.0])
    if raw_spaced:
        parts.append(fuzz.WRatio(raw_spaced, canonical) / 100.0)
        parts.append(fuzz.token_set_ratio(raw_spaced, canonical) / 100.0)
    ac = _word_acronym(canonical)
    if ac:
        parts.append(_ratio(raw_c, ac))
        parts.append(fuzz.ratio(raw_c, ac) / 100.0)
    return max(parts)

def _ordered_subsequence(small: str, big: str) -> bool:
    if not small:
        return True
    j = 0
    for ch in big:
        if j < len(small) and ch == small[j]:
            j += 1
    return j == len(small)

def _unique_subsequence_district(raw_c: str) -> str | None:
    if len(raw_c) not in (3, 4) or not raw_c.isalpha():
        return None
    hits = [name for name in CANONICAL_CROP_DISTRICTS if _ordered_subsequence(raw_c, alnum_compact(name))]
    if len(hits) == 1:
        return hits[0]
    return None

def _best_prefix_match(raw_c: str, canon_compacts: list[tuple[str, str]]) -> str | None:
    if len(raw_c) < 3:
        return None
    hits = [name for name, cc in canon_compacts if cc.startswith(raw_c)]
    if len(hits) == 1:
        return hits[0]
    return None

def resolve_crop_district(raw: str | None) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    raw_c = alnum_compact(text)
    if not raw_c:
        return None
    raw_spaced = space_fold(text)
    short_token = len(raw_c) <= 6
    min_fuzzy = 0.75 if len(raw_c) >= 8 else 0.82
    ambiguity_margin = 0.018 if len(raw_c) >= 8 else 0.03
    dot_hit = _match_dot_initials_remainder(text)
    if dot_hit:
        return dot_hit
    canon_compacts: list[tuple[str, str]] = [(c, alnum_compact(c)) for c in CANONICAL_CROP_DISTRICTS]
    for name, cc in canon_compacts:
        if raw_c == cc:
            return name
    for name, cc in canon_compacts:
        ac = _word_acronym(name)
        if ac and raw_c == ac:
            return name
    subseq = _unique_subsequence_district(raw_c)
    if subseq:
        return subseq
    prefix_hit = _best_prefix_match(raw_c, canon_compacts)
    if prefix_hit:
        return prefix_hit
    scores: list[tuple[str, float]] = []
    for name, _ in canon_compacts:
        scores.append((name, _fuzzy_against_canonical(raw_c, raw_spaced, name, short_token=short_token)))
    scores.sort(key=lambda x: -x[1])
    best_name, best = scores[0]
    second = scores[1][1] if len(scores) > 1 else 0.0
    if best < min_fuzzy:
        return None
    if best - second < ambiguity_margin and second >= min_fuzzy - 0.06:
        return None
    return best_name
