import requests
import os

base_url_auth = 'https://auth-qa.charismabi.space'

class AuthAPI():

    @staticmethod
    def api_auth_get_token(email_auth: str, password_auth: str) -> dict:

        payload = {
            "email": email_auth,
            "password": password_auth
        }

        response = requests.post(f'{base_url_auth}/accounts/api/token', json=payload)
        return response.json()

    @staticmethod
    def api_auth_create_user(name_user: str, email_user: str, user_name: str, password_user: str, image_auth: str, token_auth: str) -> dict:

        payload = {
            "name": name_user,
            "email": email_user,
            "username": user_name,
            "password": password_user,
            "image": image_auth
        }

        headers = {'Authorization': f'Bearer {token_auth}'}

        response = requests.post(f"{base_url_auth}/accounts/api/user", json=payload, headers=headers)
        return response.json()
    
    @staticmethod
    def api_auth_update_user(user_id: str, name: str = '', email: str = '', username: str = '', image: str = '') -> dict:
        
        payload = {
            "name": name, 
            "email": email, 
            "username": username,
            "image": image
        }

        headers = {'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzExOTE2OTA5LCJpYXQiOjE3MTE5MTY2MDksImp0aSI6IjQ3ZWVhZjBkZDdhZjQ1ZTE5MzhlNDNjYjFmZDU5N2M2IiwidXNlcl9pZCI6MiwidXNlcm5hbWUiOiJJdmFuMjQiLCJuYW1lIjoiSXZhbiBDYW1wZWxvIiwiZW1haWwiOiJpdmFuY2FtcGVsbzE5NzNAZ21haWwuY29tIiwiY29tcGFueSI6IkVtcHJlc2EgdGVzdGUgU1x1MDBlM28iLCJjb21wYW55X2VtYWlsIjoibHVjYXMuc2lsdmFAZ21haWwuY29tIiwiY29tcGFueV9pZCI6MSwiY29tcGFueV9sb2dvIjoiaHR0cHM6Ly9jaG0tYXV0aC1xYS5zMy5hbWF6b25hd3MuY29tL21pZGlhLzEyMmM5OWFkLTcxYy5wbmc_WC1BbXotQWxnb3JpdGhtPUFXUzQtSE1BQy1TSEEyNTYmWC1BbXotQ3JlZGVudGlhbD1BU0lBM0FHTDNRR1U1WkRPRzRFWSUyRjIwMjQwMzMxJTJGdXMtZWFzdC0xJTJGczMlMkZhd3M0X3JlcXVlc3QmWC1BbXotRGF0ZT0yMDI0MDMzMVQyMDIzMzBaJlgtQW16LUV4cGlyZXM9MzYwMCZYLUFtei1TaWduZWRIZWFkZXJzPWhvc3QmWC1BbXotU2VjdXJpdHktVG9rZW49SVFvSmIzSnBaMmx1WDJWakVBMGFDWFZ6TFdWaGMzUXRNU0pJTUVZQ0lRRFREbTglMkJtR2pkU0IlMkZQMGFpRjB6ajc3MHVScnpGRiUyQkNvQSUyRiUyRkpYVDVWUll3SWhBS0I1aGRQS1MlMkZhOHdoeWlGTnVPRWFBVzZRcG1xNUNQdjFrVHo5ZFZ1WUd3S3Z3RUNEWVFBQm9NTnpVMk16UXhOelk0TmpFM0lnd3Q3b25Wa0hLek1xWmpsUmtxMlFRdyUyQkpEYXhSUFk4czZlVHdrRVd0ejMzdVJGcGxBTlNqeVRROU41Slpkcm82QUolMkZUdEhOdmc2THVZWFgwdmZPZTBWTzIxMiUyQmFaSkNHNWxMMlVYd3ljc0prd2VGeThGNnAlMkJVRCUyQmhmNXZPVDV6V1NzSkhHNVhUekg4Ymk3bnJxcU1pMjclMkJWbmdBTUVxN1pDMHJPUHNTbUt4cm1qRXhRTTRld1AlMkJTemFSWDk1dDNwb0RrZXdsQmlxb3dMcVB2amh4S3B1M0hWU2tDZCUyQlhVY0FJRzNvTVEwbyUyRkU5cGp1a2xGaHVlJTJCZ09POGdOVTBmQW4wJTJGUFJNdDVHZG9FbnAyNVFoRm5URFRsYndEdVYlMkIlMkJMOFhzRzI2V2hkNXVJZUU5UVBaeXh1NTlORlAxWjEzekdXdXd1VWMybUczbjRtaERndUNTMmZKVUh0QlZJJTJGeGwlMkJ3RHBqT3hJOFpiMGVQTEtmZUROaUtCOHZrUnJYZWNra1hoVE5VT0xVRm9XT3dEazBpMlV3SDI5Z1lQUlVaWkpKc2FmcjRZNVRaTXlVcEJlS0tESUt6d2lZS0ZTOHkzOXRUZGdwck1hV1poYk95SkhRV0tNQm5rbFo0MlJQQ20zQVVQQldrU3RTNE9QJTJGamxCZXc2NGV5R0Q2Mm1lWE5kYnR6akRROWh0a1U4NGQlMkZRYklsRklqbE10bXNrZSUyQkVtSWE3YXE3aE9meGppJTJGUk91cGNsengwbyUyRjM5ZlNCNHNtVHdlaUN3OE8xWVNJaWVuT3JjdGc2WG5yeVhhZ3dVa0NIZUZ0Zmt6d3VkQmJJU1dwSk55biUyQlRZVzRUZGYlMkZqUkVMaGtqNHRCMCUyRnlCVDBBU21ZJTJCeGdVRSUyRmVWUUltdHpmRUQ3Qk1XN0JOWlM1d3FVVzlrcjlsSXhNT1pxQ3VMTGZSVjJJandmaVMzJTJGZzRNJTJGbERGVTBrb1JTJTJCTDhPQ2ZNYkVVaUpmaGR0VDlWZ1ZqYndmNDhNQWglMkZjVUQ1U0xxSHJZdEdSVEQlMkJhcnRpSjJDMXVPbmJtZ1hHTDYzcG9CdUpJUkR3VWlhWEVtOGlHJTJGOFdHcjEwciUyQmxGQk1NS01wN0FHT3BrQjB6cXFKM3lKUGhJRTYlMkZTR3FrZU03JTJCeGtDY1Z0RUc2akZib3F6NGQycG5zeWRYVWZrSFhBYWM2b1lNdSUyRnVIaDdxZmZQWUtXWHRKYkd1bDNZRnByTkEzMVVYeDdWT1MxWG1KQ29KT1RPbGRKdGNWNklqNWZyVjc2M2VreVlUJTJGR3J4em50M3B5aHV2Wnp2TmU3ZzdpME0xczlQTDRhaDZ6Y05sMSUyRk1hYjlHdnhReUl3QTZYTlBBaFFReDdwdSUyRnMzS2k2Z0liNGE4a2prSyZYLUFtei1TaWduYXR1cmU9OGFhNDk3ODNmNDViYjMzNmJhODI3MGU5ZjlmN2JhOTJhNDgyNTIzODYzNTRiMTFlMmU3NjlhZDQxNmYxYzE5YyIsInJvbGUiOiJhZG1fYWNjZXNzIiwiZGFzaF9pZnJhbWUiOiJodHRwczovL2xvb2tlcnN0dWRpby5nb29nbGUuY29tL2VtYmVkL3JlcG9ydGluZy9mMjRmODhkMC02ZDg4LTQwMWEtYTBhMS1iNTE4MTUxMDc5MmIvcGFnZS9wX2lxM3o4YmUxNWMiLCJwcm9kdWN0cyI6WyIxMjM0NDUiLCJ0ZXN0ZXByb2R1dG8xMjM0IiwiaXZhbi1wcm9kdWN0cyJdfQ.gT50hu3Uin_UJBJTsc86rS0KXGeqT_iOQebnBb9TRsU'}
        response = requests.put(f"{base_url_auth}/accounts/api/user/{user_id}/", json=payload, headers=headers)
        print(response.url)
        return response.json()

    @staticmethod
    def api_auth_delete_user(user_id: str, token_auth: str):
        headers = {'Authorization': f'Bearer {token_auth}'}

        response = requests.delete(f"{base_url_auth}/accounts/api/user/{user_id}/", headers=headers)
        return response.json()
    

    @staticmethod
    def api_auth_create_role(company_id: int, user_id: int, role: str, token_auth: str) -> dict:

        payload = {
            "user": user_id, 
            "company": company_id,
            "role": role
        }

        headers = {'Authorization': f'Bearer {token_auth}'}


        response = requests.post(f"{base_url_auth}/company/role/", json=payload, headers=headers)
        return response.json()


    @staticmethod
    def api_auth_reset_password(email: str) -> dict:
        payload = {
            "email": email
        }

        response = requests.post(f"{base_url_auth}/accounts/api/password-reset", json=payload)
        return response.json()


    @staticmethod
    def api_auth_confirm_reset_password(uid, token, new_password) -> dict:
        payload = {
            "uid": uid, 
            "token": token, 
            "password": new_password
        }

        response = requests.post(f"{base_url_auth}/accounts/api/password-reset/confirm", json=payload)
        return response.json()