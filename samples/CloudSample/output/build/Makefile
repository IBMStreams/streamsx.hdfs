# NOTE: This makefile is designed to be run from one level up in the directory
# hierarchy compared to where it resides. For instance, if this makefile is
# located at 'output/build/Makefile', then it can be launched as:
# 'make -f build/Makefile' from the 'output' directory.

SHELL=/bin/bash
ifeq ($(CXX),)
  CXX=g++
endif
ifeq ($(CXX_LN),)
  CXX_LN=g++
endif
PKGCONFIG := /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/bin/dst-pe-pkg-config.sh
SPL_BUNDLE_BUILDER := /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/system/impl/bin/spl-bundle-builder
SO_INCLUDE := $(shell $(PKGCONFIG) --cflags dst-spl-pe-install)
SO_LIB := $(shell $(PKGCONFIG) --libs dst-spl-pe-install)
SO_STAND_LIB := $(shell $(PKGCONFIG) --libs dst-spl-pe-install-standalone)
SPL_CXXFLAGS = -Wall -Wno-unused -Wno-sign-compare
SPL_CXXFLAGS += -D_REENTRANT -fPIC -finline-functions -fmessage-length=0
SPL_CXXFLAGS += $(SO_INCLUDE) -O3 -DNDEBUG  -std=gnu++98 
SPL_LDFLAGS = $(SO_LIB)

SPL_TYPES = build/type/BeJwrMcw0T84vzStJLQIAGEIEBG.o \
            build/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.o \
            build/type/BeJwrMSw2zcnMSy0GABBPANp.o \
            build/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.o \
            build/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.o 
SPL_FUNCTIONS = 
SPL_OPERATORS = build/operator/pulse.o \
                build/operator/CreateLinesFileNames.o \
                build/operator/HdfsFileSink.o \
                build/operator/PrintHdfsSink.o \
                build/operator/HdfsDirectoryScan.o \
                build/operator/PrintDirectoryScan.o \
                build/operator/HdfsFileSource.o \
                build/operator/PrintHdfsFileSource.o 
SPL_SO_BINS = bin/pulse.so \
              bin/CreateLinesFileNames.so \
              bin/HdfsFileSink.so \
              bin/PrintHdfsSink.so \
              bin/HdfsDirectoryScan.so \
              bin/PrintDirectoryScan.so \
              bin/HdfsFileSource.so \
              bin/PrintHdfsFileSource.so 

SPL_ADL_FILE = CloudSample.adl

SPL_OUTPUT_DIR_ROOT = $(shell printf '%q' '/home/nouriahm/workspace/streamsx.hdfs/samples/CloudSample')
SPL_OUTPUT_DIR = $(shell printf '%q' 'output')

.PHONY: distclean clean all types functions operators sos bundle toolkits standalone sablink

all: bin types functions operators sos standalone bundle sablink

distclean clean:
	@rm -fr bin/* build/type build/function build/operator build/bundle build/standalone CloudSample.sab

bin:
	@mkdir -m 775 bin

types: $(SPL_TYPES)

build/type/BeJwrMcw0T84vzStJLQIAGEIEBG.o: \
	    src/type/BeJwrMcw0T84vzStJLQIAGEIEBG.h \
	    src/type/BeJwrMcw0T84vzStJLQIAGEIEBG.cpp \
	    build/cppOptions \
	    | build/type
	@echo ' [CXX-type] tuple<int32 counter>'
	@$(CXX) -o $@ -c $(SPL_CXXFLAGS) src/type/BeJwrMcw0T84vzStJLQIAGEIEBG.cpp

build/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.o: \
	    src/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.h \
	    src/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.cpp \
	    build/cppOptions \
	    | build/type
	@echo ' [CXX-type] tuple<rstring out_file_name,uint64 size>'
	@$(CXX) -o $@ -c $(SPL_CXXFLAGS) src/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.cpp

build/type/BeJwrMSw2zcnMSy0GABBPANp.o: \
	    src/type/BeJwrMSw2zcnMSy0GABBPANp.h \
	    src/type/BeJwrMSw2zcnMSy0GABBPANp.cpp \
	    build/cppOptions \
	    | build/type
	@echo ' [CXX-type] tuple<rstring lines>'
	@$(CXX) -o $@ -c $(SPL_CXXFLAGS) src/type/BeJwrMSw2zcnMSy0GABBPANp.cpp

build/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.o: \
	    src/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.h \
	    src/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.cpp \
	    build/cppOptions \
	    | build/type
	@echo ' [CXX-type] tuple<rstring fileNames>'
	@$(CXX) -o $@ -c $(SPL_CXXFLAGS) src/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.cpp

build/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.o: \
	    src/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.h \
	    src/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.cpp \
	    build/cppOptions \
	    | build/type
	@echo ' [CXX-type] tuple<rstring line,rstring filename>'
	@$(CXX) -o $@ -c $(SPL_CXXFLAGS) src/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.cpp

build/type:
	-@mkdir -m 775 build/type

operators: $(SPL_OPERATORS)

-include build/operator/pulse.d
build/operator/pulse.o: \
	    src/operator/pulse.h \
	    src/operator/pulse.cpp\
	    build/dep/operator/pulse.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] pulse'
	@$(CXX) -o $@ -MD -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/impl/nl/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/pulse.cpp

-include build/operator/CreateLinesFileNames.d
build/operator/CreateLinesFileNames.o: \
	    src/operator/CreateLinesFileNames.h \
	    src/operator/CreateLinesFileNames.cpp\
	    build/dep/operator/CreateLinesFileNames.dep \
	    src/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.h  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] CreateLinesFileNames'
	@$(CXX) -o $@ -MD -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/impl/nl/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/CreateLinesFileNames.cpp

-include build/operator/HdfsFileSink.d
build/operator/HdfsFileSink.o: \
	    src/operator/HdfsFileSink.h \
	    src/operator/HdfsFileSink.cpp\
	    build/dep/operator/HdfsFileSink.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] HdfsFileSink'
	@$(CXX) -o $@ -MD -I ../../../../streamsx.hdfs/com.ibm.streamsx.hdfs/impl/nl/include -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/spl/utility/JavaOp/../../../impl/include -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/HdfsFileSink.cpp

-include build/operator/PrintHdfsSink.d
build/operator/PrintHdfsSink.o: \
	    src/operator/PrintHdfsSink.h \
	    src/operator/PrintHdfsSink.cpp\
	    build/dep/operator/PrintHdfsSink.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] PrintHdfsSink'
	@$(CXX) -o $@ -MD -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/impl/nl/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/PrintHdfsSink.cpp

-include build/operator/HdfsDirectoryScan.d
build/operator/HdfsDirectoryScan.o: \
	    src/operator/HdfsDirectoryScan.h \
	    src/operator/HdfsDirectoryScan.cpp\
	    build/dep/operator/HdfsDirectoryScan.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] HdfsDirectoryScan'
	@$(CXX) -o $@ -MD -I ../../../../streamsx.hdfs/com.ibm.streamsx.hdfs/impl/nl/include -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/spl/utility/JavaOp/../../../impl/include -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/HdfsDirectoryScan.cpp

-include build/operator/PrintDirectoryScan.d
build/operator/PrintDirectoryScan.o: \
	    src/operator/PrintDirectoryScan.h \
	    src/operator/PrintDirectoryScan.cpp\
	    build/dep/operator/PrintDirectoryScan.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] PrintDirectoryScan'
	@$(CXX) -o $@ -MD -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/impl/nl/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/PrintDirectoryScan.cpp

-include build/operator/HdfsFileSource.d
build/operator/HdfsFileSource.o: \
	    src/operator/HdfsFileSource.h \
	    src/operator/HdfsFileSource.cpp\
	    build/dep/operator/HdfsFileSource.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] HdfsFileSource'
	@$(CXX) -o $@ -MD -I ../../../../streamsx.hdfs/com.ibm.streamsx.hdfs/impl/nl/include -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/spl/utility/JavaOp/../../../impl/include -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/HdfsFileSource.cpp

-include build/operator/PrintHdfsFileSource.d
build/operator/PrintHdfsFileSource.o: \
	    src/operator/PrintHdfsFileSource.h \
	    src/operator/PrintHdfsFileSource.cpp\
	    build/dep/operator/PrintHdfsFileSource.dep  \
	    build/cppOptions \
	    | build/operator
	@echo ' [CXX-operator] PrintHdfsFileSource'
	@$(CXX) -o $@ -MD -I /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/toolkits/spl/impl/nl/include -DSPLJAVAFUNCTION_ADL_FILENAME=\"CloudSample.adl\" -c $(SPL_CXXFLAGS) src/operator/PrintHdfsFileSource.cpp

build/operator:
	-@mkdir -m 775 -p build/operator

functions: $(SPL_FUNCTIONS)

sos: $(SPL_SO_BINS)

SOpulse = \
	    build/operator/pulse.o \
	    build/type/BeJwrMcw0T84vzStJLQIAGEIEBG.o 

bin/pulse.so: \
	$(SOpulse) \
	build/dep/pulse.so \
	build/ldOptions
	@echo ' [LD-so] so pulse'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/pulse.so -fPIC -shared $(SOpulse) $(SPL_LDFLAGS)

SOCreateLinesFileNames = \
	    build/operator/CreateLinesFileNames.o \
	    build/type/BeJwrMcw0T84vzStJLQIAGEIEBG.o \
	    build/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.o 

bin/CreateLinesFileNames.so: \
	$(SOCreateLinesFileNames) \
	build/dep/CreateLinesFileNames.so \
	build/ldOptions
	@echo ' [LD-so] so CreateLinesFileNames'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/CreateLinesFileNames.so -fPIC -shared $(SOCreateLinesFileNames) $(SPL_LDFLAGS)

SOHdfsFileSink = \
	    build/operator/HdfsFileSink.o \
	    build/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.o \
	    build/type/BeJwrMSo2ycnMSy22SMvMSc1LzE0FAD_0kAbi.o 

bin/HdfsFileSink.so: \
	$(SOHdfsFileSink) \
	build/dep/HdfsFileSink.so \
	build/ldOptions
	@echo ' [LD-so] so HdfsFileSink'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/HdfsFileSink.so -fPIC -shared $(SOHdfsFileSink) -lstreams-stdtk-javaop -L /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/jre/lib/amd64/j9vm -Wl,-rpath -Wl,/opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/jre/lib/amd64/j9vm -lpthread -ldl -lstreams-stdtk-runtime $(SPL_LDFLAGS)

SOPrintHdfsSink = \
	    build/operator/PrintHdfsSink.o \
	    build/type/BeJwrMSo2NM4vLYlPy8xJjc9LzE0NNSnOrEoFAG67AkZ.o 

bin/PrintHdfsSink.so: \
	$(SOPrintHdfsSink) \
	build/dep/PrintHdfsSink.so \
	build/ldOptions
	@echo ' [LD-so] so PrintHdfsSink'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/PrintHdfsSink.so -fPIC -shared $(SOPrintHdfsSink) $(SPL_LDFLAGS)

SOHdfsDirectoryScan = \
	    build/operator/HdfsDirectoryScan.o \
	    build/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.o 

bin/HdfsDirectoryScan.so: \
	$(SOHdfsDirectoryScan) \
	build/dep/HdfsDirectoryScan.so \
	build/ldOptions
	@echo ' [LD-so] so HdfsDirectoryScan'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/HdfsDirectoryScan.so -fPIC -shared $(SOHdfsDirectoryScan) -lstreams-stdtk-javaop -L /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/jre/lib/amd64/j9vm -Wl,-rpath -Wl,/opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/jre/lib/amd64/j9vm -lpthread -ldl -lstreams-stdtk-runtime $(SPL_LDFLAGS)

SOPrintDirectoryScan = \
	    build/operator/PrintDirectoryScan.o \
	    build/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.o 

bin/PrintDirectoryScan.so: \
	$(SOPrintDirectoryScan) \
	build/dep/PrintDirectoryScan.so \
	build/ldOptions
	@echo ' [LD-so] so PrintDirectoryScan'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/PrintDirectoryScan.so -fPIC -shared $(SOPrintDirectoryScan) $(SPL_LDFLAGS)

SOHdfsFileSource = \
	    build/operator/HdfsFileSource.o \
	    build/type/BeJwrMSw2zcnMSy0GABBPANp.o \
	    build/type/BeJwrMSy2TMvMSfVLzE0tBgAhJgTm.o 

bin/HdfsFileSource.so: \
	$(SOHdfsFileSource) \
	build/dep/HdfsFileSource.so \
	build/ldOptions
	@echo ' [LD-so] so HdfsFileSource'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/HdfsFileSource.so -fPIC -shared $(SOHdfsFileSource) -lstreams-stdtk-javaop -L /opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/jre/lib/amd64/j9vm -Wl,-rpath -Wl,/opt/ibm/InfoSphere_Streams/4.3.0.0.20180417160227/java/jre/lib/amd64/j9vm -lpthread -ldl -lstreams-stdtk-runtime $(SPL_LDFLAGS)

SOPrintHdfsFileSource = \
	    build/operator/PrintHdfsFileSource.o \
	    build/type/BeJwrMSw2zcnMSy0GABBPANp.o 

bin/PrintHdfsFileSource.so: \
	$(SOPrintHdfsFileSource) \
	build/dep/PrintHdfsFileSource.so \
	build/ldOptions
	@echo ' [LD-so] so PrintHdfsFileSource'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/PrintHdfsFileSource.so -fPIC -shared $(SOPrintHdfsFileSource) $(SPL_LDFLAGS)

standalone: bin/standalone.exe

build/standalone/standalone.o: \
	    src/standalone/standalone.h \
	    src/standalone/standalone.cpp  \
	    build/cppOptions \
	    | build/standalone
	@echo ' [CXX-standalone] standalone'
	@$(CXX) -o $@ -c $(SPL_CXXFLAGS) src/standalone/standalone.cpp

bin/standalone.exe: build/standalone/standalone.o build/ldOptions
	@echo ' [LD-standalone] standalone'
	@$(CXX_LN) $(SPL_CXXFLAGS) -o bin/standalone.exe build/standalone/standalone.o $(SO_STAND_LIB)
	@echo ' [LN-standalone] standalone '
	@ln -sf standalone bin/CloudSample
build/standalone:
	-@mkdir -m 775 build/standalone

sablink:

	@ln -sf ../CloudSample.sab bin/sab

bundle: CloudSample.sab

-include build/dep/CloudSample.sab.d
CloudSample.sab: $(SPL_SO_BINS) $(SPL_ADL_FILE) build/dep/CloudSample.sab.d bin/standalone.exe
	@echo ' [Bundle] CloudSample.sab'
	@$(SPL_BUNDLE_BUILDER) $(SPL_OUTPUT_DIR_ROOT) $(SPL_OUTPUT_DIR) $(SPL_ADL_FILE) CloudSample.sab

