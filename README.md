# NBA-API-Data-Getter
Gets player season stats with sqlite into a .db file around 23,000 lines

HOW TO USE:
1. Let the get data.py file run, it will automatically handle errors and give you info on whats going on
2. At around ever 600 players it will start to time out. This script will automatically retry three times and move one. However these retrys take very long. Solution: after it skips one player, stop the script. Wait a couple of minutes then restart the script. It will automatically skip the players it already has data for so no need to worry about anything. Repeat this whenever it starts to time out.

DISCLAIMER:
You may need to change the file path variables as it could be set up different for you
