from helpers import RE_GARANTIE, RE_ID, RE_VIEWS, clean_phone, normalize_url, parse_price


def test_clean_phone_variants():
    assert clean_phone("+40 723 456 789") == "0723456789"
    assert clean_phone("0723-456-789") == "0723456789"


def test_parse_price():
    assert parse_price("109 €") == ("109", "€")
    assert parse_price("5 000 Lei") == ("5000", "RON")


def test_normalize_url():
    u = "https://www.olx.ro/d/oferta/xyz.html?utm_source=mail&reason=observed&id=123#frag"
    assert normalize_url(u) == "https://www.olx.ro/d/oferta/xyz.html?id=123"


def test_regex_id_views_garantie():
    text = "ID: 123456\nVizualizari: 1.234\nGarantie (RON): 5 000"
    assert RE_ID.search(text).group(1) == "123456"
    assert RE_VIEWS.search(text).group(1).replace(".", "").strip() == "1234"
    assert "5000" in RE_GARANTIE.search(text).group(1).replace(" ", "")
