unit:
	pytest -v

coverage:
	pytest --cov=antismash_models --cov-report=html --cov-report=term-missing

corelint:
	flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics

lint:
	flake8 . --count --exit-zero --max-complexity=20 --statistics
