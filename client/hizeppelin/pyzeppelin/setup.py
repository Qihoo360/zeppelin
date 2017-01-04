from distutils.core import setup, Extension
setup(name = 'pyzeppelin', version = '0.0.1', ext_modules = [Extension('pyzeppelin', sources=['pyzeppelin.cc'],
     include_dirs=['../../', '../../../third/pink/'],
     extra_objects=['../../output/lib/libzp.a', '../../../third/pink/output/lib/libpink.a','/usr/local/lib/libprotobuf.so'],
     extra_compile_args=['-std=c++11', '-lprotobuf', '-lpink']
     )])
