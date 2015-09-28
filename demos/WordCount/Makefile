##################################################################### 
# Copyright (C)2014, International Business Machines Corporation and 
# others. All Rights Reserved. 
#####################################################################

STREAMS_SPLPATH ?= ../../com.ibm.streamsx.hdfs
SPLC_FLAGS = -a --spl-path $(STREAMS_SPLPATH) --data-directory=data

#-t ../com.ibm.streamsx.hdfs -t ../../com.ibm.streamsx.hdfs

SPLC = $(STREAMS_INSTALL)/bin/sc

SPL_CMD_ARGS ?=
SPL_MAIN_COMPOSITE = WordCount
STANDALONE_OUTPUT_DIR = output/Standalone
DISTRIBUTED_OUTPUT_DIR = output/Distributed
SOURCES = WordCount.spl ReadAndTokenize.spl

all: distributed

standalone: output/Standalone/bin/standalone

distributed: output/Distributed/Main.sab

output/Standalone/bin/standalone: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -T -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR) $(SPL_CMD_ARGS) 

output/Distributed/Main.sab: $(SOURCES)
	$(SPLC) $(SPLC_FLAGS) -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR) $(SPL_CMD_ARGS)

clean: clean-standalone clean-distributed

clean-standalone:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(STANDALONE_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml

clean-distributed:
	$(SPLC) $(SPLC_FLAGS) -C -M $(SPL_MAIN_COMPOSITE) --output-dir ./$(DISTRIBUTED_OUTPUT_DIR)/$(SPL_MAIN_COMPOSITE)
	-rm -rf toolkit.xml
