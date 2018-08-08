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
SPL_COMP1NAME=HDFSBinaryRead
SPL_COMP2NAME=HDFSBinaryWrite
SPL_MAIN_COMPOSITE1 = com.ibm.streamsx.hdfs.sample::$(SPL_COMP1NAME)
SPL_MAIN_COMPOSITE2 = com.ibm.streamsx.hdfs.sample::$(SPL_COMP2NAME)
BUILD_OUTPUT_DIR = output


all: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP1NAME) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP2NAME) $(SPL_CMD_ARGS)

data:
	mkdir data
clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP1NAME)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP2NAME)
	-rm -rf toolkit.xml
	-rm -rf data/FileContents.txt
	-rm -rf data/FileContentsInputPort.txt
