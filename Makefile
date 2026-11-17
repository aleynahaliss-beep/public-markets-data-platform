.PHONY: setup data run test docs clean

setup:            ## install dependencies (Java 17+ must be available for Spark)
	pip install -r requirements.txt
	pip install -e .

data:             ## regenerate the synthetic raw feeds
	python -m pmdp.generate_data

run:              ## generate feeds if missing, run the whole pipeline
	python -m pmdp.pipeline

test:             ## unit tests + end-to-end regression
	python -m pytest -q

docs:             ## regenerate the DQ rule matrix from the rule definitions
	python scripts/gen_rule_matrix.py

clean:
	rm -rf data output .pytest_cache
