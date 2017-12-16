package io.zrz.jpgsql.proto.wire;

public enum AuthType {

  AuthenticationMD5Password(5)

  ;

  private int authType;

  AuthType(int authType) {
    this.authType = authType;
  }

  public static AuthType fromInt(int val) {
    switch (val) {
      case 5:
        return AuthType.AuthenticationMD5Password;
    }
    return null;
  }

}
