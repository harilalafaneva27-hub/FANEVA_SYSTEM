"""
FANEVA IA — détection de langue et libellés UI (FR / MG)
=======================================================
Module pur, sans Kivy ni SQLite. Utilisé par l'UI et par FanevaIA.
"""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

# Indices malagasy courants (mots / caractères)
_MG_MARKERS = re.compile(
    r"(?i)\b("
    r"inona|ahoana|aiza|oviana|manao|mivarotra|stoka|tahiry|trosa|vola|"
    r"tombony|varotra|isan['']?andro|andaniny|misy|tsy|aho|ianao|"
    r"ampy|ambony|ambany|farany|andiany|fanontaniana|valiny|aza|"
    r"mba|ary|sy|na|amin['']?ny|ny|ireo|ireto"
    r")\b"
)

# Indices français
_FR_MARKERS = re.compile(
    r"(?i)\b("
    r"quel|quelle|quels|quelles|comment|combien|pourquoi|où|quand|"
    r"analyse|stock|vente|ventes|marge|marges|dette|dettes|prévision|"
    r"prevision|rapport|aujourd['']hui|semaine|mois|produit|caisse"
    r")\b"
)


def detect_language(text: str) -> str:
    """Retourne 'mg' ou 'fr' (défaut fr si indécis)."""
    t = (text or "").strip()
    if not t:
        return "fr"
    mg = len(_MG_MARKERS.findall(t))
    fr = len(_FR_MARKERS.findall(t))
    # Caractères typiques malagasy (ô, etc. rares) — fallback mots
    if mg > fr:
        return "mg"
    if fr > mg:
        return "fr"
    # Heuristique légère : beaucoup de "ny "/"sy " → mg
    low = t.lower()
    if low.count(" ny ") + low.count(" sy ") >= 2 and mg >= 1:
        return "mg"
    return "fr"


def language_instruction(lang: str) -> str:
    if lang == "mg":
        return (
            "Valio amin'ny teny malagasy fotsiny. "
            "Aza mampiasa teny frantsay raha tsy ilaina tokoa ny anarana teknika."
        )
    return (
        "Réponds en français uniquement. "
        "N'utilise le malagasy que si un nom propre l'exige."
    )


# Libellés UI bilingues (écran Assistant)
UI_STRINGS: Dict[str, Dict[str, str]] = {
    "fr": {
        "menu": "🤖 FANEVA IA",
        "title": "🤖 Assistant FANEVA",
        "greeting": "Bonjour 👋\nQue voulez-vous savoir ?",
        "hint": "Posez votre question...",
        "send": "ENVOYER",
        "back": "RETOUR",
        "thinking": "Analyse en cours…",
        "unavailable": "FANEVA IA est indisponible pour le moment. L'application continue de fonctionner normalement.",
        "timeout": "Délai dépassé. Réessayez plus tard.",
        "no_key": "Clé API absente — IA non configurée. L'application reste utilisable.",
        "error": "Impossible d'obtenir une réponse IA.",
        "empty": "Veuillez saisir une question.",
        "sug_sales": "📊 Analyse des ventes",
        "sug_stock": "📦 Analyse du stock",
        "sug_margins": "💰 Analyse des marges",
        "sug_debts": "💳 Analyse des dettes",
        "sug_forecast": "📈 Prévisions",
        "q_sales": "Analyse les ventes du jour et résume les points importants.",
        "q_stock": "Analyse le stock : ruptures, stock faible et total.",
        "q_margins": "Analyse les marges et le bénéfice sur la période du jour.",
        "q_debts": "Analyse les dettes actives et les montants restants.",
        "q_forecast": "Donne des prévisions prudentes basées uniquement sur les données fournies.",
        "readonly_note": "Mode lecture seule — aucune modification métier.",
    },
    "mg": {
        "menu": "🤖 FANEVA IA",
        "title": "🤖 Mpanampy FANEVA",
        "greeting": "Salama 👋\nInona no tianao ho fantatra ?",
        "hint": "Asio ny fanontanianao...",
        "send": "ALEFA",
        "back": "MIVERINA",
        "thinking": "Eo am-pandinihana…",
        "unavailable": "Tsy misy FANEVA IA amin'izao. Mbola mandeha normal ny rindrambaiko.",
        "timeout": "Lany fotoana. Andramo indray azafady.",
        "no_key": "Tsy misy API key — tsy voaamboatra ny IA. Mbola azo ampiasaina ny rindrambaiko.",
        "error": "Tsy azo ny valiny avy amin'ny IA.",
        "empty": "Asio fanontaniana azafady.",
        "sug_sales": "📊 Famakafakana varotra",
        "sug_stock": "📦 Famakafakana stoka",
        "sug_margins": "💰 Famakafakana tombony",
        "sug_debts": "💳 Famakafakana trosa",
        "sug_forecast": "📈 Vinavina",
        "q_sales": "Diniho ny varotra androany ary fintino ny zava-dehibe.",
        "q_stock": "Diniho ny stoka: lany, ambany, ary fitambarany.",
        "q_margins": "Diniho ny tombony sy ny margin androany.",
        "q_debts": "Diniho ny trosa mbola misy sy ny sisa aloa.",
        "q_forecast": "Omeo vinavina mitandrina mifototra amin'ny angona omena ihany.",
        "readonly_note": "Vakiana fotsiny — tsy misy fanovana momba ny varotra.",
    },
}


def ui_text(key: str, lang: str = "fr") -> str:
    pack = UI_STRINGS.get(lang) or UI_STRINGS["fr"]
    return pack.get(key) or UI_STRINGS["fr"].get(key, key)


def suggestion_pairs(lang: str = "fr") -> Tuple[Tuple[str, str], ...]:
    """Liste (libellé bouton, question préremplie)."""
    return (
        (ui_text("sug_sales", lang), ui_text("q_sales", lang)),
        (ui_text("sug_stock", lang), ui_text("q_stock", lang)),
        (ui_text("sug_margins", lang), ui_text("q_margins", lang)),
        (ui_text("sug_debts", lang), ui_text("q_debts", lang)),
        (ui_text("sug_forecast", lang), ui_text("q_forecast", lang)),
    )


def map_error_message(error_code: Optional[str], lang: str = "fr") -> str:
    code = (error_code or "").upper()
    if code in ("PROVIDER_UNAVAILABLE", "PROVIDER_DISABLED"):
        return ui_text("unavailable", lang)
    if code == "TIMEOUT":
        return ui_text("timeout", lang)
    if code in ("MISSING_API_KEY",):
        return ui_text("no_key", lang)
    if code == "EMPTY_QUESTION":
        return ui_text("empty", lang)
    return ui_text("error", lang)
