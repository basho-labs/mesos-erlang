#!/bin/bash

cfg="tickTime=$tickTime\n\n"
cfg="${cfg}initLimit=$initLimit\n\n"
cfg="${cfg}syncLimit=$syncLimit\n\n"
cfg="${cfg}clientPort=$clientPort\n\n"
cfg="${cfg}dataDir=$DIR/$DATA_DIR\n\n"
cfg="${cfg}dataLogDir=$DIR/$LOG_DIR\n\n"

echo "Creating $DIR/conf/zoo.cfg..."

echo -e "$cfg" > "$DIR/conf/zoo.cfg"

"$DIR/bin/zkServer.sh" start-foreground
