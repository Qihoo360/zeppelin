#!/usr/bin/env python
# -*- coding: utf-8 -*-

# test_zp.py
import pyzeppelin


b = pyzeppelin.create_client("your ip", port, "tab")
(s, msg) = pyzeppelin.connect(b) ## 不能跳过这一步
if s != 0:
    print msg
else:
    test_key   = "key——bada——大海"
    test_value = "value_中文字符"
    (s, msg) = pyzeppelin.set(b, test_key, test_value)
    if s != 0:
        print msg
        (s, msg) = pyzeppelin.remove_client(b)

    (s, msg) = pyzeppelin.get(b, test_key)
    if s == 0 or s== 1: # s==1 msg ==None; the key not found
        result = msg
        print "get value:", result
    else:
        print msg
        (s, msg) = pyzeppelin.remove_client(b)

    (s, msg) = pyzeppelin.delete(b, "key")
    if s != 0:
        print msg
        (s, msg) = pyzeppelin.remove_client(b)
