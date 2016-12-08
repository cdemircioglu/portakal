set mytime=%time%
echo Current time is %mytime% > Upload.txt


git add .
git commit -m "Code update"
git push -u origin master