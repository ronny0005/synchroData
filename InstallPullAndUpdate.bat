C:

::SCHTASKS /CREATE /XML "C:\ItSolution\ItSolution\SynchroData\UpdateData Task.xml" /RU "NT AUTHORITY\SYSTEM" /TN "ItSolution\UpdateData task"

C:\ItSolution\ItSolution\SynchroData\UpdateDatabase_jar\UpdateDatabase.bat

set /p DUMMY=Hit ENTER to continue...