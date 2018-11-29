##################################################################### 
# Copyright (C)2014, 2018 International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################     
                            
PHONY: all 

TOOLKIT_NAME=com.ibm.streamsx.hdfs
STREAMS_HDFS_TOOLKIT ?= $(shell ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
			         ([ -e "../../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../../$(TOOLKIT_NAME)) ||\
			         ([ -e "../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../$(TOOLKIT_NAME)) ||\
			           echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))

SPLC_FLAGS = -a --data-directory data

SPL_CMD_ARGS ?= -t $(STREAMS_HDFS_TOOLKIT)

SPLC = $(STREAMS_INSTALL)/bin/sc
SPL_MAIN_COMPOSITE = application::CloudSample
BUILD_OUTPUT_DIR = output


all: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(BUILD_OUTPUT_DIR) $(SPL_CMD_ARGS)

data:
	mkdir data
clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(BUILD_OUTPUT_DIR)

