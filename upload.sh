git diff --check
python3 -m pytest

rm -rf build dist
python3 -m build
python3 -m twine check dist/*
ls -lh dist

git add .
git commit -m "Prepare Frontier Runner 0.2.4 release"
git push origin main

git status --short
git log -1 --oneline
git rev-parse HEAD
git rev-parse origin/main

git tag -a v0.2.4 -m "Frontier Runner 0.2.4"
git push origin v0.2.4