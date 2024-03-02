import logging

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

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
        username: str = payload.get("sub")
        if username is None:
            # raise credentials_exception
            return None
    except JWTError:
        # raise credentials_exception
        return None
    return username
