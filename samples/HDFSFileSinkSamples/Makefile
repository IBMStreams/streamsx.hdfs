##################################################################### 
# Copyright (C)2014, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

.PHONY: all clean

STREAMS_SPLPATH ?=../../com.ibm.streamsx.hdfs
SPLC_FLAGS = -a -z --spl-path $(STREAMS_SPLPATH):../com.ibm.streamsx.hdfs

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE1 = HDFSFileSinkSampleLineFormat
SPL_MAIN_COMPOSITE2 = HDFSFileSinkSampleTxtFormat
SPL_MAIN_DYNAMIC = DynamicFilename
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed


all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M hdfsexample::$(SPL_MAIN_COMPOSITE1) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS) 
	$(SPLC) $(SPLC_FLAGS) -T -M hdfsexample::$(SPL_MAIN_COMPOSITE2) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -T -M hdfsexample::$(SPL_MAIN_DYNAMIC) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)

distributed:
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_MAIN_COMPOSITE1) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_MAIN_COMPOSITE2) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_MAIN_DYNAMIC) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_COMPOSITE1) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_COMPOSITE2) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_DYNAMIC) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_COMPOSITE1) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_COMPOSITE2) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_DYNAMIC) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)
	-rm -rf toolkit.xml