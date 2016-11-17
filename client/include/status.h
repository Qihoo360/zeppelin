#ifndef STATUS_H
#define STATUS_H

#include <string>

class Status {
  public:
    enum Code {
      kOk,
			kNotFound,
	  	kWait,
      kErr
    };

    Status(int32_t code = kOk, const std::string& msg = "") :
      code_(code),
      msg_(msg) {
    }

    virtual ~Status() {}

    bool ok() {
      return (code_ == kOk);
    }

    void SetOk(const std::string& msg = "", const std::string& value = "") {
      code_ = kOk;
      msg_ = msg;
			value_ = value;
    }

    void SetErr(const std::string& msg = "", const std::string& value = "") {
      code_ = kErr;
      msg_ = msg;
			value_ = value;
    }

		void SetWait(const std::string& msg = "", const std::string& value = "") {
			code_ = kWait;
			msg_ = msg;
			value_ = value;
		}
		
		void Set(int32_t code, const std::string& msg = "", const std::string& value = "") {
			code_ = code;
			msg_ = msg;
			value_ = value;
		}

    int32_t code() {
      return code_;
    }

    std::string msg() {
      return msg_;
    }

		std::string value() {
			return value_;
		}

		void set_code(int32_t code) {
			code_ = code;
		}
		void set_msg(std::string& msg) {
			msg_ = msg;
		}
		void set_value(std::string& value) {
			value_ = value;
		}
  private:
    int32_t code_;
    std::string msg_;
	  std::string value_;	
};

#endif
