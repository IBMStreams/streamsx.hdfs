##################################################################### 
# Copyright (C)2014, 2018 International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

.PHONY: all clean

STREAMS_SPLPATH ?=../../com.ibm.streamsx.hdfs:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.hdfs  --data-directory data 
SPLC_FLAGS = -a -z --spl-path $(STREAMS_SPLPATH)

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE1 = HDFS2FileSinkSampleLineFormat
SPL_MAIN_COMPOSITE2 = HDFS2FileSinkSampleTxtFormat
SPL_MAIN_DYNAMIC = DynamicFilename
BUILD_OUTPUT_DIR = output


all: data
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_MAIN_COMPOSITE2) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2) $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M hdfsexample::$(SPL_MAIN_DYNAMIC) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)

data:
	mkdir data
clean:
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE1)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_COMPOSITE2) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE2)
	$(SPLC) $(SPLC_FLAGS) -C -M hdfsexample::$(SPL_MAIN_DYNAMIC) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_DYNAMIC) $(SPL_CMD_ARGS)
	-rm -rf toolkit.xml
