##################################################################### 
# Copyright (C)2014, 2018 International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################  
                            
.PHONY: all

TOOLKIT_NAME=com.ibm.streamsx.hdfs
STREAMS_HDFS_TOOLKIT ?= $(shell ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
			         ([ -e "../../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../../$(TOOLKIT_NAME)) ||\
			         ([ -e "../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../$(TOOLKIT_NAME)) ||\
			           echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))


ifdef STREAMS_BUILDSERVICE
  STREAMS_HDFS_TOOLKIT = ../toolkits:$(STREAMS_INSTALL)/toolkits
endif

SPLC_FLAGS = -a -t $(STREAMS_HDFS_TOOLKIT) --data-directory data

SPL_CMD_ARGS ?= 

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_COMP1NAME=HdfsFileCopy
SPL_MAIN_COMPOSITE1 = hdfsexample::$(SPL_COMP1NAME)
BUILD_OUTPUT_DIR = output

all: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP1NAME)  $(SPL_CMD_ARGS)

data: 
	mkdir data
clean:	
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP1NAME)
	-rm -f toolkit.xml
	-rm -rf data/work2
	-rm -rf output

