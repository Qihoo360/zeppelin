dnl $Id$
dnl config.m4 for extension zeppelin

dnl Comments in this file start with the string 'dnl'.
dnl Remove where necessary. This file will not work
dnl without editing.

dnl If your extension references something external, use with:

dnl PHP_ARG_WITH(zeppelin, for zeppelin support,
dnl Make sure that the comment is aligned:
dnl [  --with-zeppelin             Include zeppelin support])

dnl Otherwise use enable:

PHP_ARG_ENABLE(zeppelin, whether to enable zeppelin support,
dnl Make sure that the comment is aligned:
[  --enable-zeppelin           Enable zeppelin support])

if test "$PHP_ZEPPELIN" != "no"; then
  dnl Write more examples of tests here...

  dnl # --with-zeppelin -> check with-path
  dnl SEARCH_PATH="/usr/local /usr"     # you might want to change this
  dnl SEARCH_FOR="/include/zeppelin.h"  # you most likely want to change this
  dnl if test -r $PHP_ZEPPELIN/$SEARCH_FOR; then # path given as parameter
  dnl   ZEPPELIN_DIR=$PHP_ZEPPELIN
  dnl else # search default path list
  dnl   AC_MSG_CHECKING([for zeppelin files in default path])
  dnl   for i in $SEARCH_PATH ; do
  dnl     if test -r $i/$SEARCH_FOR; then
  dnl       ZEPPELIN_DIR=$i
  dnl       AC_MSG_RESULT(found in $i)
  dnl     fi
  dnl   done
  dnl fi
  dnl
  dnl if test -z "$ZEPPELIN_DIR"; then
  dnl   AC_MSG_RESULT([not found])
  dnl   AC_MSG_ERROR([Please reinstall the zeppelin distribution])
  dnl fi

  dnl # --with-zeppelin -> add include path
  PHP_ADD_INCLUDE(../../output)
  PHP_ADD_INCLUDE(../../output/include)
  PHP_ADD_INCLUDE(../../../third/pink/output)
  PHP_ADD_INCLUDE(../../../third/pink/output/include)
  PHP_ADD_INCLUDE(../../../third/slash/output)
  PHP_ADD_INCLUDE(../../../third/slash/output/include)

  dnl # --with-zeppelin -> check for lib and symbol presence
  dnl LIBNAME=zeppelin # you may want to change this
  dnl LIBSYMBOL=zeppelin # you most likely want to change this 

  dnl PHP_CHECK_LIBRARY($LIBNAME,$LIBSYMBOL,
  dnl [
  dnl   PHP_ADD_LIBRARY_WITH_PATH($LIBNAME, $ZEPPELIN_DIR/lib, ZEPPELIN_SHARED_LIBADD)
  dnl   AC_DEFINE(HAVE_ZEPPELINLIB,1,[ ])
  dnl ],[
  dnl   AC_MSG_ERROR([wrong zeppelin lib version or lib not found])
  dnl ],[
  dnl   -L$ZEPPELIN_DIR/lib -lm
  dnl ])
  dnl
  dnl PHP_SUBST(ZEPPELIN_SHARED_LIBADD)

  PHP_REQUIRE_CXX()
  PHP_ADD_LIBRARY(stdc++, 1, EXTRA_LDFLAGS)
  PHP_ADD_LIBRARY(protobuf, 1, EXTRA_LDFLAGS)
  PHP_ADD_LIBRARY_WITH_PATH(zp, ../../output/lib, EXTRA_LDFLAGS)
  PHP_ADD_LIBRARY_WITH_PATH(zp, ../../../third/pink/output/lib, EXTRA_LDFLAGS)
  PHP_ADD_LIBRARY_WITH_PATH(zp, ../../../third/slash/output/lib, EXTRA_LDFLAGS)
  
  CPPFILE="zeppelin.cc"

  PHP_NEW_EXTENSION(zeppelin, $CPPFILE, $ext_shared)
fi
