#include <Python.h>
#include "include/zp_cluster.h"


static void zeppelin_free(void *ptr)
{
    libzp::Client *b = static_cast<libzp::Client *>(ptr);
    delete b;
    return;
}

static PyObject *create_client(PyObject *, PyObject* args)
{
   char* table = NULL;
   char* hostname = NULL;
   int port = 9866;

   if (!PyArg_ParseTuple(args, "sis", &hostname, &port, &table)) {
       return NULL;
   }
   
   libzp::Client *b = new libzp::Client(hostname, port, table);
   if (b == NULL) {
       return NULL;
   }

   return PyCObject_FromVoidPtr(b, zeppelin_free);
}

static PyObject *remove_client(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    if (!PyArg_ParseTuple(args, "O", &pyb)) {
        return Py_BuildValue("(is)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    if (b) {
      delete b;
    }
    return NULL;
}

static PyObject *connect(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    if (!PyArg_ParseTuple(args, "O", &pyb)) {
        return Py_BuildValue("(is)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    libzp::Status s = b->Connect();
    if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }
    
    return Py_BuildValue("(is)", 0, "Connect OK");
}

static PyObject *set(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    char* key = NULL;
    char* val = NULL;
    int kl=0, vl=0;

    if (!PyArg_ParseTuple(args, "Os#s#", &pyb, &key, &kl, &val, &vl)) {
        return Py_BuildValue("(is)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    libzp::Status s = b->Set(std::string(key, kl), std::string(val, vl));
    if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    return Py_BuildValue("(is)", 0, "Set OK");
}

static PyObject *get(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    char* key = NULL;
    int kl=0;

    if (!PyArg_ParseTuple(args, "Os#", &pyb, &key, &kl)) {
        return Py_BuildValue("(is#)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    std::string val;
    libzp::Status s = b->Get(std::string(key, kl), &val);
    if (s.IsNotFound()) {
        return Py_BuildValue("(is)", 1, NULL);
    } else if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    return Py_BuildValue("(is#)", 0, val.data(), val.size());
}

static PyObject *zeppelin_delete(PyObject *, PyObject* args)
{
    PyObject *pyb = NULL;
    char* key = NULL;
    int kl=0;

    if (!PyArg_ParseTuple(args, "Os#", &pyb, &key, &kl)) {
        return Py_BuildValue("(is#)", -1, "ParseTuple Failed");
    }

    void *vb = PyCObject_AsVoidPtr(pyb);
    libzp::Client *b = static_cast<libzp::Client *>(vb);
    libzp::Status s = b->Delete(std::string(key, kl));
    if (!s.ok()) {
        return Py_BuildValue("(is#)", -2, s.ToString().data(), s.ToString().size());
    }

    return Py_BuildValue("(is)", 0, "Delete OK");
}

static PyMethodDef pyzeppelin_methods[] = {
    {"create_client",       create_client,     METH_VARARGS},
    {"connect",             connect,             METH_VARARGS},
    {"set",                 set,                 METH_VARARGS},
    {"get",                 get,                 METH_VARARGS},
    {"delete",              zeppelin_delete,              METH_VARARGS},
    {"remove_client",       remove_client,              METH_VARARGS},
    {NULL, NULL}
};

PyMODINIT_FUNC initpyzeppelin (void)
{
    Py_InitModule("pyzeppelin", pyzeppelin_methods);
}

