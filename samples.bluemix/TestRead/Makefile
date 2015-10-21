##################################################################### 
# Copyright (C)2014, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

.PHONY: all clean

HDFS_BLUEMIX ?=../../com.ibm.streamsx.hdfs.bluemix
INET_TOOLKIT ?= ../../../streamsx.inet/com.ibm.streamsx.inet
SPLC_FLAGS = -a -t $(HDFS_BLUEMIX) -t $(INET_TOOLKIT)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = TestRead
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed


all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS) 

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed
	rm data/dropFirst.txt
	rm data/lines.txt

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml
