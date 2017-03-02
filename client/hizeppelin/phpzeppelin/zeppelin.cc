/*
  +----------------------------------------------------------------------+
  | PHP Version 5                                                        |
  +----------------------------------------------------------------------+
  | Copyright (c) 1997-2011 The PHP Group                                |
  +----------------------------------------------------------------------+
  | This source file is subject to version 3.01 of the PHP license,      |
  | that is bundled with this package in the file LICENSE, and is        |
  | available through the world-wide-web at the following url:           |
  | http://www.php.net/license/3_01.txt                                  |
  | If you did not receive a copy of the PHP license and are unable to   |
  | obtain it through the world-wide-web, please send a note to          |
  | license@php.net so we can mail you a copy immediately.               |
  +----------------------------------------------------------------------+
  | Author:                                                              |
  +----------------------------------------------------------------------+
*/

/* $Id: header 310447 2011-04-23 21:14:10Z bjori $ */
extern "C"
{
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
}

#include "php_zeppelin.h"
#include "include/zp_cluster.h"
#include "status.h"
#include "zend.h"

/* If you declare any globals in php_zeppelin.h uncomment this: 
ZEND_DECLARE_MODULE_GLOBALS(zeppelin)
*/
static int le_zeppelin;

/* True global resources - no need for thread safety here */
zend_class_entry *zeppelin_client_ext;

/* {{{ zeppelin_functions[]
 *
 * Every user visible function must have an entry in zeppelin_functions[].
 */

zend_function_entry zeppelin_functions[] = {
    PHP_FE(confirm_zeppelin_compiled,    NULL)        /* For testing, remove later. */
    PHP_ME(Zeppelin, __construct, NULL, ZEND_ACC_CTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, __destruct, NULL, ZEND_ACC_DTOR | ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, set, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, get, NULL, ZEND_ACC_PUBLIC)
    PHP_ME(Zeppelin, delete, NULL, ZEND_ACC_PUBLIC)
    {NULL, NULL, NULL}
    //PHP_FE_END    /* Must be the last line in zeppelin_functions[] */
};
/* }}} */ 

/* {{{ zeppelin_module_entry
 */
zend_module_entry zeppelin_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
    STANDARD_MODULE_HEADER,
#endif
    "zeppelin",
    zeppelin_functions,
    PHP_MINIT(zeppelin),
    PHP_MSHUTDOWN(zeppelin),
    PHP_RINIT(zeppelin),        /* Replace with NULL if there's nothing to do at request start */
    PHP_RSHUTDOWN(zeppelin),    /* Replace with NULL if there's nothing to do at request end */
    PHP_MINFO(zeppelin),
#if ZEND_MODULE_API_NO >= 20010901
    "0.1", /* Replace with version number for your extension */
#endif
    STANDARD_MODULE_PROPERTIES
};
/* }}} */ 

extern "C"
{
#ifdef COMPILE_DL_ZEPPELIN
ZEND_GET_MODULE(zeppelin)
#endif
}

static void zeppelin_destructor_zeppelin_client(zend_rsrc_list_entry * rsrc TSRMLS_DC)
{
	libzp::Client *zp = (libzp::Client *) rsrc->ptr;
    delete zp;
}

int zeppelin_client_get(zval *id, libzp::Client **zeppelin_sock TSRMLS_DC, int no_throw)
{
    zval **socket;
    int resource_type;

    if (Z_TYPE_P(id) != IS_OBJECT || zend_hash_find(Z_OBJPROP_P(id), "Zeppelin",
                sizeof("Zeppelin"), (void **) &socket) == FAILURE) {
        return -1;
    }

    *zeppelin_sock = (libzp::Client *) zend_list_find(Z_LVAL_PP(socket), &resource_type);

    if (!*zeppelin_sock || resource_type != le_zeppelin) {
        return -1;
    }

    return Z_LVAL_PP(socket);
}

PHP_MINIT_FUNCTION(zeppelin)
{
    /* If you have INI entries, uncomment these lines 
    REGISTER_INI_ENTRIES();
    */
    zend_class_entry zeppelin_class_entry;
    INIT_CLASS_ENTRY(zeppelin_class_entry, "Zeppelin", zeppelin_functions);
    zeppelin_client_ext = zend_register_internal_class(&zeppelin_class_entry TSRMLS_CC);

    le_zeppelin = zend_register_list_destructors_ex(
        zeppelin_destructor_zeppelin_client,
        NULL,
        "zeppelin-client", module_number
    );

    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION
 */
PHP_MSHUTDOWN_FUNCTION(zeppelin)
{
    /* uncomment this line if you have INI entries
    UNREGISTER_INI_ENTRIES();
    */
    return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request start */
/* {{{ PHP_RINIT_FUNCTION
 */
PHP_RINIT_FUNCTION(zeppelin)
{
    return SUCCESS;
}
/* }}} */

/* Remove if there's nothing to do at request end */
/* {{{ PHP_RSHUTDOWN_FUNCTION
 */
PHP_RSHUTDOWN_FUNCTION(zeppelin)
{
    return SUCCESS;
}
/* }}} */ 

/* {{{ PHP _MINFO_FUNCTION
 */
PHP_MINFO_FUNCTION(zeppelin)
{
    php_info_print_table_start();
    php_info_print_table_header(2, "zeppelin support", "enabled");
    php_info_print_table_end();

    /* Remove comments if you have entries in php.ini
    DISPLAY_INI_ENTRIES();
    */
}
/* }}} */


PHP_METHOD(Zeppelin, __construct)
{
	char *ip = NULL;
	int ip_len = 0;
	int port = 0;
	char *table = NULL;
	int table_len = 0;
	zval *self;
	zval *object;
	int id;

	libzp::Client *zp = NULL;
	if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Osls",
				&object, zeppelin_client_ext, &ip, &ip_len, &port, &table, &table_len) == FAILURE) {
		RETURN_FALSE;
	}
	zp = new libzp::Client(std::string(ip, ip_len), port, std::string(table, table_len));

#if PHP_VERSION_ID >= 50400
	id = zend_list_insert(zp, le_zeppelin TSRMLS_CC);
#else
	id = zend_list_insert(zp, le_zeppelin);
#endif
	add_property_resource(object, "Zeppelin", id);
}


PHP_METHOD(Zeppelin, __destruct)
{
    // todo
}

PHP_FUNCTION(confirm_zeppelin_compiled)
{
    char *arg = NULL;
    int arg_len, len;
    char *strg;

    if (zend_parse_parameters(ZEND_NUM_ARGS() TSRMLS_CC, "s", &arg, &arg_len) == FAILURE) {
        return;
    }

    len = spprintf(&strg, 0, "Congratulations! You have successfully modified ext/%.78s/config.m4. Module %.78s is now compiled into PHP.", "zeppelin", arg);
    RETURN_STRINGL(strg, len, 0);
}

PHP_METHOD(Zeppelin, set)
{
    char *key   = NULL;
    char *value = NULL;
    int argc = ZEND_NUM_ARGS();
    int key_len   = 0;
    int value_len = 0;
    zval *object;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Oss",
                &object, zeppelin_client_ext, &key, &key_len, &value, &value_len) == FAILURE) {
        return;
    }
    
	libzp::Client *zp;
    if(zeppelin_client_get(object, &zp TSRMLS_CC, 0) < 0) {
        RETURN_FALSE;
    }
   
	libzp::Status s = zp->Set(std::string(key, key_len), std::string(value, value_len));
    if (s.ok()) {
        RETVAL_TRUE;
    } else {
        RETVAL_FALSE;
    }
}

PHP_METHOD(Zeppelin, get)
{
    struct timeval tv;
    char *key   = NULL;
    int argc = ZEND_NUM_ARGS();
    int key_len   = 0;
    zval *object;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Os",
                &object, zeppelin_client_ext, &key, &key_len) == FAILURE) {
        return;
    }
    
	libzp::Client *zp;
    if(zeppelin_client_get(object, &zp TSRMLS_CC, 0) < 0) {
        RETURN_FALSE;
    }
    
    std::string val;
	libzp::Status s = zp->Get(std::string(key, key_len), &val);
	if (s.ok()) {
        RETVAL_STRINGL((char *)val.data(), val.size(), 1);
	} else {
        RETVAL_FALSE;
	}
}

PHP_METHOD(Zeppelin, delete)
{
    char *key   = NULL;
    int argc = ZEND_NUM_ARGS();
    int key_len   = 0;
    zval *object;
    if (zend_parse_method_parameters(ZEND_NUM_ARGS() TSRMLS_CC, getThis(), "Os",
                &object, zeppelin_client_ext, &key, &key_len) == FAILURE) {
        return;
    }
    
	libzp::Client *zp;
    if(zeppelin_client_get(object, &zp TSRMLS_CC, 0) < 0) {
        RETURN_FALSE;
    }
    
	libzp::Status s = zp->Delete(std::string(key, key_len));
    if (s.ok()) {
        RETVAL_TRUE;
    } else {
        RETVAL_FALSE;
    }
}

/* The previous line is meant for vim and emacs, so it can correctly fold and 
   unfold functions in source code. See the corresponding marks just before 
   function definition, where the functions purpose is also documented. Please 
   follow this convention for the convenience of others editing your code.
*/

/* The previous line is meant for vim and emacs, so it can correctly fold and 
   unfold functions in source code. See the corresponding marks just before 
   function definition, where the functions purpose is also documented. Please 
   follow this convention for the convenience of others editing your code.
*/


/*
 * Local variables:

/* The previous line is meant for vim and emacs, so it can correctly fold and 
   unfold functions in source code. See the corresponding marks just before 
   function definition, where the functions purpose is also documented. Please 
   follow this convention for the convenience of others editing your code.
*/


/*
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: noet sw=4 ts=4 fdm=marker
 * vim<600: noet sw=4 ts=4
 */
