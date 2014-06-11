# Copyright (C) 2013-2014, International Business Machines Corporation  
# All Rights Reserved      
                            
.PHONY: all clean

SPLC_FLAGS = -a -z -t ../../com.ibm.streamsx.hdfs

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE1 = hdfsexample::HDFSFileSinkSampleLineFormat
SPL_MAIN_COMPOSITE2 = hdfsexample::HDFSFileSinkSampleTxtFormat
SPL_MAIN_DYNAMIC = hdfsexample::DynamicFilename
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed

all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) 
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_DYNAMIC) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)

distributed:
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_DYNAMIC) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	-rm -rf toolkit.xml
	
clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	-rm -rf toolkit.xml