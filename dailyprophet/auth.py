"""
Consider doing this instead: https://developers.google.com/identity/gsi/web/guides/verify-google-id-token

from google.oauth2 import id_token
from google.auth.transport import requests

try:
    idinfo = id_token.verify_oauth2_token(token, requests.Request(), CLIENT_ID)
"""

import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from jose.exceptions import ExpiredSignatureError

# OAuth2 scheme to get the token from the Authorization header
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Secret key to sign and verify the JWT token
SECRET_KEY = "secret"  # not signed by me

logger = logging.getLogger(__name__)


def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        options = {"verify_signature": False, "verify_aud": False}
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"], options=options)
        logger.debug(payload)
        user: str = payload.get("sub")
        if user is None:
            # raise credentials_exception
            return None
    except ExpiredSignatureError:
        options = {"verify_signature": False, "verify_aud": False, "verify_exp": False}
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"], options=options)
        user: str = payload.get("sub")
        logger.warning(f"Expire token for {user}")
        if user is None:
            # raise credentials_exception
            return None
    except JWTError as e:
        # raise credentials_exception
        logger.error(e)
        return None
    except Exception as e:
        logger.error(e)
        return None
    return user


if __name__ == "__main__":
    from .configs import TEST_BEARER_TOKEN

    token = TEST_BEARER_TOKEN
    username = get_current_user(token)
    print(username)
