cd /D "%~dp0"
set anacondapath="C:\Users\duynd13\Anaconda3"
set datetimef='%date:~-4%-%date:~-10,-8%-%date:~-7,-5%'
set scriptpath="%~dp0\main_daily.py"

call %anacondapath%"\Scripts\activate.bat"
%anacondapath%"\python.exe" %scriptpath% %datetimef%