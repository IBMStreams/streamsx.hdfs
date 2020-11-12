#####################################################################
# Copyright (C)2014, 2016 International Business Machines Corporation and
# others. All Rights Reserved.
#####################################################################

.PHONY: all clean

TOOLKIT_NAME=com.ibm.streamsx.hdfs
STREAMS_HDFS_TOOLKIT ?= $(shell ([ -e "../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../$(TOOLKIT_NAME)) ||\
			         ([ -e "../../../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../../../$(TOOLKIT_NAME)) ||\
			         ([ -e "../$(TOOLKIT_NAME)/toolkit.xml" ] && echo ../$(TOOLKIT_NAME)) ||\
			           echo $(STREAMS_INSTALL)/toolkits/$(TOOLKIT_NAME))

ifdef STREAMS_BUILDSERVICE
  STREAMS_HDFS_TOOLKIT = ../toolkits:$(STREAMS_INSTALL)/toolkits
endif

SPLC_FLAGS = -a -t $(STREAMS_HDFS_TOOLKIT) --data-directory data

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_COMP1NAME=webHDFS
SPL_MAIN_COMPOSITE1 = application::$(SPL_COMP1NAME)
BUILD_OUTPUT_DIR = output

all: data clean
	$(SPLC) $(SPLC_FLAGS) -M  $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)  $(SPL_CMD_ARGS)

data:
	mkdir data
clean:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir output
	-rm -f toolkit.xml
	-rm -rf data
	-rm -rf output

