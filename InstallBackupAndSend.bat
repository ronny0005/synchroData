IF NOT EXIST "C:\ItSolution" (
	7-ZipPortable\App\7-Zip64\7z.exe x ItSolution.zip -oc:\
)

C:

call C:\ItSolution\ItSolution\SynchroData\ConfigFile_jar\ConfigFile.bat

SCHTASKS /DELETE /TN "ItSolution\BackupData task" /f
SCHTASKS /CREATE /XML "C:\ItSolution\ItSolution\SynchroData\BackupData Task.xml" /RU "NT AUTHORITY\SYSTEM" /TN "ItSolution\BackupData task"

call C:\ItSolution\ItSolution\SynchroData\UpdateDatabase_jar\UpdateDatabase.bat

set /p DUMMY=Hit ENTER to continue...