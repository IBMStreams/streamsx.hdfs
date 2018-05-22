##################################################################### 
# Copyright (C)2014, 2018 International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################  
                            
.PHONY: all clean
 
SPLC_FLAGS = -a -t ../../com.ibm.streamsx.hdfs:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.hdfs  --data-directory data 

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_COMP1NAME=HDFS2FileSourceSampleLineFormat
SPL_COMP2NAME=HDFS2FileSourceSampleTxtFormat
SPL_MAIN_COMPOSITE1 = hdfsexample::$(SPL_COMP1NAME)
SPL_MAIN_COMPOSITE2 = hdfsexample::$(SPL_COMP2NAME)
BUILD_OUTPUT_DIR = output

all: data
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP1NAME)  $(SPL_CMD_ARGS)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP2NAME) $(SPL_CMD_ARGS)

data: 
	mkdir data
clean:	
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE1) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP1NAME)
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE2) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_COMP2NAME)
	-rm -rf toolkit.xml
	-rm -rf data/LineSink.txt
	-rm -rf data/TxtSink.txt


