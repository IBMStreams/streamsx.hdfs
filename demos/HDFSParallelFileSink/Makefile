##################################################################### 
# Copyright (C)2014, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

STREAMS_SPLPATH ?= ..:../..
SPLC_FLAGS = -a -z --spl-path $(STREAMS_SPLPATH)
#-t ../com.ibm.streamsx.hdfs -t ../../com.ibm.streamsx.hdfs

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = parallelwrite::Main
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed
SOURCES = parallelwrite/Main.spl

all: distributed

standalone: output/Standalone/Main/bin/standalone

distributed: output/Distributed/Main/parallelwrite.Main.adl

output/Standalone/Main/bin/standalone: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/Main $(SPL_CMD_ARGS) 

output/Distributed/Main/parallelwrite.Main.adl: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/Main $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml