def calculate_revenue(quantity, unit_price):
    return quantity * unit_price

def test_revenue():
    assert calculate_revenue(3, 100.0) == 300.0

def test_zero_quantity():
    assert calculate_revenue(0, 100.0) == 0
