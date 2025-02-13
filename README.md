# database-agent

Agent binary Synchronize the Mysql Databases

It fetches Db status, Users roles and their privileges

command to build the agent


go build -o authnull-db-agent main.go


File Path for db.env 


/etc/authnull-db-agent/db.env


File Path for authnull-db-agent.log file


/var/log/authnull-db-agent.log


Command to run agent 

./authnull-db-agent -host "localhost" -username "root" -password "password"


