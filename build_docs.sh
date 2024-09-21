#!/bin/bash
cd docs/
make html
cp -r build/html/* .
cd ..
git add .
git commit -m "Update documentation"
git push