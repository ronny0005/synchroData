C:

:: SCHTASKS /CREATE /XML "C:\ItSolution\ItSolution\SynchroData\BackupData Task.xml" /RU "NT AUTHORITY\SYSTEM" /TN "ItSolution\BackupData task"

C:\ItSolution\ItSolution\SynchroData\UpdateDatabase_jar\UpdateDatabase.bat

set /p DUMMY=Hit ENTER to continue...