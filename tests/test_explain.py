from stl.explain import explain


def test_deterministic_explanation_names_the_flip():
    text = explain(use_ollama=False)
    assert "Cy" in text and "150.00" in text
    assert "Bo" in text and "140.00" in text
    assert "watermark" in text.lower()
