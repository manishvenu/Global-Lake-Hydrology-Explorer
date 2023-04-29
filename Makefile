init:
    conda create --name GlobalLakeHydrologyExplorer --file requirements.txt

test:
    py.test tests

.PHONY: init test