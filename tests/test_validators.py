import random

from pyspark.sql import functions as F

from pmdp.spark_validators import add_isin_helpers, isin_is_valid
from pmdp.validators import is_valid_isin, isin_check_digit


def test_known_valid_isin():
    assert is_valid_isin("US0378331005")


def test_wrong_check_digit_is_invalid():
    assert not is_valid_isin("US0378331006")


def test_format_violations_are_invalid():
    assert not is_valid_isin(None)
    assert not is_valid_isin("")
    assert not is_valid_isin("us0378331005")      # lower case
    assert not is_valid_isin("US037833100")       # too short
    assert not is_valid_isin("US03783310055")     # too long


def test_generated_check_digit_round_trips():
    body = "DE000ABC123"
    assert is_valid_isin(body + str(isin_check_digit(body)))


def test_spark_isin_expression_agrees_with_python_reference(spark):
    rng = random.Random(7)
    alnum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    samples = ["US0378331005", "US0378331006", "us0378331005", "US037833100", "US03783310055", "", None, "XX", "!!0378331005"]
    for _ in range(300):
        body = rng.choice(["US", "GB", "DE", "FR", "AE"]) + "".join(rng.choices(alnum, k=9))
        valid = body + str(isin_check_digit(body))
        samples.append(valid)                                                   # valid
        samples.append(body + str((int(valid[-1]) + rng.randint(1, 9)) % 10))   # wrong check digit
        samples.append(valid[:-1] + "Z")                                        # non-digit check character
    df = add_isin_helpers(spark.createDataFrame([(s,) for s in samples], "isin string"))
    got = {r["isin"]: r["ok"] for r in df.select("isin", isin_is_valid("isin").alias("ok")).collect()}
    for s in samples:
        assert got[s] == is_valid_isin(s), f"disagreement on {s!r}"
