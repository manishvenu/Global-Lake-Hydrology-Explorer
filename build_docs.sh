#!/bin/bash
cd docs/
make html
cp -r _build/html/* .
cd ..
git add .
git commit -m "Update documentation"
git push