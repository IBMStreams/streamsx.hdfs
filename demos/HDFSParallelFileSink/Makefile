##################################################################### 
# Copyright (C)2014, 2018 International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

SPLC_FLAGS = -a -z -t ../../com.ibm.streamsx.hdfs:$(STREAMS_INSTALL)/toolkits/com.ibm.streamsx.hdfs

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = parallelwrite::Main
BUILD_OUTPUT_DIR = output
SOURCES = parallelwrite/Main.spl

all: output/Main/parallelwrite.Main.adl


output/Main/parallelwrite.Main.adl: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(BUILD_OUTPUT_DIR)/Main $(SPL_CMD_ARGS)

clean: 
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(BUILD_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml
