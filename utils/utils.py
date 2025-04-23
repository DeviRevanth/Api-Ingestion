#######################################################
#Name: Kola Devi Revanth
#Date: 23-04-2025
#Version: 2.0
#Version Comments: Updated Version
#Objective: Enabling and to send email notifications using mailx for plain text and sendmail for HTML with attachments.
#userstory:
########################################################

#### Importing Necessary Packages ####
import os
import subprocess
import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import logging
import logging.config

def setup_logger(file_name :str, log_config :str=None):
    """
    A method to setup logger object

    Parameters:
    filename (str)   : Log file name
    log_config (str) : Log config for the given log file name

    Returns:
    logger (object) : logger object is returned where on which logging can be performed
    """
    default_log_config = {
        "version": 1,
        "formatters": {
            "mirroring": {
                "format": "%(asctime)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "mirroring",
                "filename": file_name,
                "mode": "w",
                "level": "DEBUG"
            }
        },
        "root": {
            "handlers": ['file'],
            "level": "DEBUG"
        }
    }
    if log_config:
        default_log_config.update(log_config)
    logging.config.dictConfig(default_log_config)
    logging.captureWarnings(True)
    return logging.getLogger(file_name)

def send_email_notification(subject :str, message :str, logger :object, log_path :str='', email_stake_holders :str="email", add_on_email_stake_holders :str='', is_html :bool=False, attachments :list=None) -> None:
    """
    Sends an email notification to specified recipients. Supports both plain text and HTML emails, 
    with optional attachments.

    Parameters:
        subject (str): The subject of the email.
        message (str): The body of the email. Can be plain text or HTML based on the `is_html` flag.
        logger (logging.Logger): Logger instance for logging email sending status and errors.
        log_path (str, optional): Path to a log file to attach to the email. Defaults to an empty string.
        email_stake_holders (str, optional): Primary email recipients, separated by commas. Defaults to "ITOPSCDOODPIngestionSupport@gehealthcare.com".
        add_on_email_stake_holders (str, optional): Additional email recipients, separated by commas. Defaults to an empty string.
        is_html (bool, optional): Flag to indicate if the email body is HTML formatted. Defaults to False.
        attachments (list, optional): List of file paths to attach to the email. Defaults to None.

    Raises:
        Exception: Logs and raises an exception if email sending fails.

    Notes:
        - For plain text emails, the `mailx` command is used.
        - For HTML emails with attachments, the `sendmail` command is used.
        - Ensure that the required system utilities (`mailx` and `sendmail`) are installed and configured properly.
    """
    try:
        recipients :str= email_stake_holders
        if add_on_email_stake_holders:
            recipients += f",{add_on_email_stake_holders}"
        logger.info(f"Sending email to: {recipients}")
        files_to_attach :list= attachments or []
        if log_path:
            files_to_attach.append(log_path)
        if is_html: # Use sendmail for HTML emails
            msg = MIMEMultipart()
            msg["To"] = recipients
            msg["Subject"] = subject
            msg.attach(MIMEText(message, "html"))
            for file_path in files_to_attach:
                if os.path.exists(file_path):
                    ctype, encoding = mimetypes.guess_type(file_path)
                    if ctype is None or encoding is not None:
                        ctype = "application/octet-stream"
                    maintype, subtype = ctype.split("/", 1)
                    with open(file_path, "rb") as f:
                        part = MIMEBase(maintype, subtype)
                        part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header("Content-Disposition", f'attachment; filename="{os.path.basename(file_path)}"')
                        msg.attach(part)
            subprocess.run(["/usr/sbin/sendmail", "-t", "-oi"], input=msg.as_string().encode(), check=True)
            logger.info("HTML email with attachments sent successfully using sendmail.")
        else: # Use mailx for plain text
            if log_path:
                os.system(f"""echo "{message}" | mailx -s "{subject}" -a {log_path}  {recipients}""")
            else:
                os.system(f"""echo "{message}" | mailx -s "{subject}" {recipients}""")
            logger.info("Plain text email sent successfully using mailx.")
        logger.info("Email alert has been sent to above mentioned email recipients")
    except Exception as err:
        logger.error(f"Failed to send email notification: {err}")
