IF NOT EXIST "C:\ItSolution" (

7-ZipPortable\App\7-Zip64\7z.exe x ItSolution.zip -oc:\

)

C:

call C:\ItSolution\ItSolution\SynchroData\ConfigFile_jar\ConfigFile.bat

SCHTASKS /CREATE /XML "C:\ItSolution\ItSolution\SynchroData\UpdateData Task.xml" /RU "NT AUTHORITY\SYSTEM" /TN "ItSolution\UpdateData task"

C:\ItSolution\ItSolution\SynchroData\UpdateDatabase_jar\UpdateDatabase.bat

set /p DUMMY=Hit ENTER to continue...