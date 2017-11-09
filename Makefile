unit:
	pytest -v

coverage:
	pytest --cov=antismash_models --cov-report=html --cov-report=term-missing
