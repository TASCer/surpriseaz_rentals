cd /D "D:\PycharmProjects\hoa_insights\src"
echo %cd%
call conda activate py311_5
python.exe "main.py"
if NOT ["%errorlevel%"] == ["0"] pause