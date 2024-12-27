import argparse
import asyncio
import base64
import hashlib
import os
import random
import re
import string
import subprocess
import tempfile
import time
from functools import partial

from bittensor.utils.btlogging import logging as logger
from bs4 import BeautifulSoup
from jsmin import jsmin


# Obfuscator base class
class Obfuscator:
    @staticmethod
    def generate_random_string(length=8):
        return "".join(
            random.choices(string.ascii_letters, k=1)
            + random.choices(string.ascii_letters + string.digits, k=length - 1)
        )

    @staticmethod
    def simple_encrypt(text, key):
        return base64.b64encode(bytes([ord(c) ^ key for c in text])).decode()

    @classmethod
    def obfuscate(cls, content):
        raise NotImplementedError("Subclasses must implement this method")


# Obfuscator for HTML content
# Encrypts the HTML content and generates a JavaScript snippet to decrypt it
# The JavaScript snippet is then embedded in the HTML content
class HTMLObfuscator(Obfuscator):
    @staticmethod
    def generate_random_string(length=8):
        return "".join(
            random.choices(string.ascii_letters, k=1)
            + random.choices(string.ascii_letters + string.digits, k=length - 1)
        )

    @staticmethod
    def simple_encrypt(text, key):
        return base64.b64encode(bytes(c ^ key for c in text.encode())).decode()

    @classmethod
    def obfuscate(cls, html_content):
        soup = BeautifulSoup(html_content, "html.parser")
        scripts = soup.find_all("script")

        # Obfuscate the remaining HTML
        body_content = str(soup.body)
        body_content = re.sub(r"\s+", " ", body_content).replace("> <", "><")

        encryption_key = random.randint(1, 255)
        encrypted_content = cls.simple_encrypt(body_content, encryption_key)

        decrypt_func, result_var = (
            cls.generate_random_string(),
            cls.generate_random_string(),
        )

        js_code = (
            f"function {decrypt_func}(e,t){{try{{var r=atob(e),n='';for(var i=0;i<r.length;i++){{n+=String.fromCharCode(r.charCodeAt(i)^t)}}return n}}catch(err){{console.error('Decryption failed:',err);return e}}}}"
            f"var {result_var}={decrypt_func}('{encrypted_content}',{encryption_key});"
            f"if({result_var}.indexOf('<')!==-1){{document.body.innerHTML={result_var};}}else{{console.error('Decryption produced invalid HTML');document.body.innerHTML=atob('{encrypted_content}');}}"
        )

        new_script = soup.new_tag("script")
        new_script.string = js_code

        soup.body.clear()
        soup.body.append(new_script)
        soup.body.extend(scripts)

        return str(soup)


# Obfuscator for JavaScript code
# Uses UglifyJS to minify and obfuscate the JavaScript code
class JSObfuscator(Obfuscator):
    UGLIFYJS_COMMAND = [
        "uglifyjs",
        "--compress",
        "--mangle",
        "--mangle-props",
        "--toplevel",
    ]
    MAX_RETRIES = 5
    RETRY_DELAY = 1
    TIMEOUT = 3

    @staticmethod
    def is_uglifyjs_available():
        try:
            subprocess.run(["uglifyjs", "--version"], capture_output=True, check=True)
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            return False

    @staticmethod
    def simple_minify(js_code):
        return jsmin(js_code)

    @classmethod
    def obfuscate(cls, js_code):
        if cls.is_uglifyjs_available():
            with tempfile.NamedTemporaryFile(
                mode="w+", suffix=".js", delete=True
            ) as temp_file:
                temp_file.write(js_code)
                temp_file.flush()

                for attempt in range(cls.MAX_RETRIES):
                    try:
                        result = subprocess.run(
                            cls.UGLIFYJS_COMMAND + [temp_file.name],
                            capture_output=True,
                            text=True,
                            check=True,
                            timeout=cls.TIMEOUT,
                        )
                        return result.stdout
                    except subprocess.TimeoutExpired:
                        logger.warning(
                            f"Attempt {attempt + 1} timed out after {cls.TIMEOUT} seconds. Retrying..."
                        )
                    except subprocess.CalledProcessError as e:
                        logger.warning(f"Attempt {attempt + 1} failed: {e}")
                        logger.warning(f"UglifyJS stderr: {e.stderr}")
                    except Exception as e:
                        logger.warning(
                            f"Attempt {attempt + 1} failed with unexpected error: {str(e)}"
                        )

                    if attempt < cls.MAX_RETRIES - 1:
                        time.sleep(cls.RETRY_DELAY)
                    else:
                        logger.error(
                            f"All {cls.MAX_RETRIES} attempts to obfuscate with UglifyJS failed. Falling back to simple minification."
                        )
                        return cls.simple_minify(js_code)
        else:
            logger.warning("UglifyJS not found. Falling back to simple minification.")
            return cls.simple_minify(js_code)


async def obfuscate_html_and_js(html_content, timeout=30):
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(
                None, partial(_obfuscate_html_and_js_sync, html_content)
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        logger.error(f"Obfuscation timed out after {timeout} seconds")
        return html_content  # Return original content if obfuscation times out


def _obfuscate_html_and_js_sync(html_content):
    logger.info("Obfuscating HTML and JavaScript content...")
    soup = BeautifulSoup(html_content, "html.parser")

    # Obfuscate JavaScript content
    for script in soup.find_all("script"):
        if script.string:
            obfuscated_js = JSObfuscator.obfuscate(script.string)
            script.string = obfuscated_js

    obfuscated_html = str(soup)
    final_obfuscated_html = HTMLObfuscator.obfuscate(obfuscated_html)
    logger.info("Obfuscation complete")
    return final_obfuscated_html


async def process_file(input_file: str, output_file: str):
    try:
        with open(input_file, encoding="utf-8") as file:
            original_content = file.read()
    except FileNotFoundError:
        logger.error(f"Error: The file '{input_file}' was not found.")
        return
    except OSError:
        logger.error(f"Error: Could not read the file '{input_file}'.")
        return

    obfuscated = await obfuscate_html_and_js(original_content)

    try:
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(obfuscated)
        logger.info(f"Obfuscated content has been written to '{output_file}'")

        # Calculate and display hashes to show difference
        original_hash = hashlib.md5(original_content.encode()).hexdigest()
        obfuscated_hash = hashlib.md5(obfuscated.encode()).hexdigest()
        logger.info(f"\nOriginal content MD5: {original_hash}")
        logger.info(f"Obfuscated content MD5: {obfuscated_hash}")
    except OSError:
        logger.error(f"Error: Could not write to the file '{output_file}'.")


# Function to test the obfuscation
# Command to run: python obfuscation_utils.py input.html
async def main():
    parser = argparse.ArgumentParser(
        description="Obfuscate HTML and JavaScript content"
    )
    parser.add_argument("input_file", help="Path to the input HTML file")
    parser.add_argument(
        "-o", "--output", help="Path to the output obfuscated HTML file (optional)"
    )
    args = parser.parse_args()

    # Generate default output filename based on input filename
    input_filename = os.path.basename(args.input_file)
    input_name, input_ext = os.path.splitext(input_filename)
    output_file = args.output or f"{input_name}_obfuscated{input_ext}"

    await process_file(args.input_file, output_file)


if __name__ == "__main__":
    asyncio.run(main())
