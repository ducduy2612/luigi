cd /D "%~dp0"
set anacondapath="C:\Users\duynd13\Anaconda3"
set scriptpath="%~dp0\main_adjustment.py"
set datetimef='%date:~-4%-%date:~-10,-8%-%date:~-7,-5%'

call %anacondapath%"\Scripts\activate.bat"
%anacondapath%"\python.exe" %scriptpath% %datetimef% '-2'
pause