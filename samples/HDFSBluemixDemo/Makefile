##################################################################### 
# Copyright (C)2015, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

.PHONY: all clean

HDFS_TOOLKIT_LOCATION = ../../com.ibm.streamsx.hdfs
SPLC_FLAGS = -a -z -t $(HDFS_TOOLKIT_LOCATION)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_WRITE_COMPOSITE = TestWrite
SPL_READ_COMPOSITE = TestRead
SPL_SCAN_COMPOSITE = TestDirScan
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed


all: distributed

standalone:
	$(SPLC) $(SPLC_FLAGS) -T -M hdfsexample::$(SPL_WRITE_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_WRITE_COMPOSITE) $(SPL_CMD_ARGS) 
	$(SPLC) $(SPLC_FLAGS) -T -M hdfsexample::$(SPL_READ_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_READ_COMPOSITE) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -T -M hdfsexample::$(SPL_SCAN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_SCAN_COMPOSITE) $(SPL_CMD_ARGS)

distributed:
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_WRITE_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_WRITE_COMPOSITE) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_READ_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_READ_COMPOSITE) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_SCAN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_SCAN_COMPOSITE) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_WRITE_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_WRITE_COMPOSITE)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_READ_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_READ_COMPOSITE)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_SCAN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_SCAN_COMPOSITE) $(SPL_CMD_ARGS)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_WRITE_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_READ_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_READ_COMPOSITE)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_SCAN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_SCAN_COMPOSITE) $(SPL_CMD_ARGS)
	-rm -rf toolkit.xml