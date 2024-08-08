"""
DDP tiktok module
"""

from pathlib import Path
import logging
import zipfile
import re
import io

import pandas as pd

import port.unzipddp as unzipddp
from port.validate import (
    DDPCategory,
    StatusCode,
    ValidateInput,
    Language,
    DDPFiletype,
)

logger = logging.getLogger(__name__)

DDP_CATEGORIES = [
    DDPCategory(
        id="txt_en",
        ddp_filetype=DDPFiletype.JSON,
        language=Language.EN,
        known_files=[
            "Transaction History.txt",
            "Most Recent Location Data.txt",
            "Comments.txt",
            "Purchases.txt",
            "Share History.txt",
            "Favorite Sounds.txt",
            "Searches.txt",
            "Login History.txt",
            "Favorite Videos.txt",
            "Favorite HashTags.txt",
            "Hashtag.txt",
            "Location Reviews.txt",
            "Favorite Effects.txt",
            "Following.txt",
            "Status.txt",
            "Browsing History.txt",
            "Like List.txt",
            "Follower.txt",
            "Watch Live settings.txt",
            "Go Live settings.txt",
            "Go Live History.txt",
            "Watch Live History.txt",
            "Profile Info.txt",
            "Autofill.txt",
            "Post.txt",
            "Block List.txt",
            "Settings.txt",
            "Customer support history.txt",
            "Communication with shops.txt",
            "Current Payment Information.txt",
            "Returns and Refunds History.txt",
            "Product Reviews.txt",
            "Order History.txt",
            "Vouchers.txt",
            "Saved Address Information.txt",
            "Order dispute history.txt",
            "Product Browsing History.txt",
            "Shopping Cart List.txt",
            "Direct Messages.txt",
            "Off TikTok Activity.txt",
            "Ad Interests.txt",
        ],
    ),
]

STATUS_CODES = [
    StatusCode(id=0, description="Valid DDP", message=""),
    StatusCode(id=1, description="Not a valid DDP", message=""),
    StatusCode(id=2, description="Bad zip", message=""),
]

def validate_zip(file: Path) -> ValidateInput:
    """
    Validates the input of a TikTok submission
    """

    validation = ValidateInput(STATUS_CODES, DDP_CATEGORIES)

    try:
        paths = []
        with zipfile.ZipFile(file, "r") as zf:
            for f in zf.namelist():
                p = Path(f)
                if p.suffix in (".txt"):
                    logger.debug("Found: %s in zip", p.name)
                    paths.append(p.name)

        validation.infer_ddp_category(paths)
        if validation.ddp_category.id == "unknown":  # pyright: ignore
            validation.set_status_code(1)
        else: 
            validation.set_status_code(0)

    except zipfile.BadZipFile:
        validation.set_status_code(2)

    return validation



def browsing_history_to_df(tiktok_zip: str) -> pd.DataFrame:

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Browsing History.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Gekeken video"])
        out = out.drop_duplicates()
        out.reset_index(drop=True, inplace=True)
        out = out[["Gekeken video", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out



def favorite_hashtag_to_df(tiktok_zip: str) -> pd.DataFrame:

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Favorite HashTags.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nHashTag Link(?::|::) (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Hashtag url"])
        out = out[["Hashtag url", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out


def favorite_videos_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Favorite Videos.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Video"])
        out = out[["Video", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out



def follower_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Follower.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nUsername: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Gebruikersnaam"])
        out = out[["Gebruikersnaam", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out



def following_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Following.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nUsername: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Gebruikersnaam"])
        out = out[["Gebruikersnaam", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out


def hashtag_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Hashtag.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Hashtag Name: (.*?)\nHashtag Link: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Hashtag naam", "Hashtag url"])
        out = out[["Gebruikersnaam", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out



def like_list_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Like List.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nLink: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Video"])
        out = out[["Video", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out


def searches_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Searches.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nSearch Term: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Zoekterm"])
        out = out[["Zoekterm", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out



def share_history_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Share History.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nShared Content: (.*?)\nLink: (.*?)\nMethod: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Gedeelde inhoud", "Url", "Gedeeld via"])
        out = out[["Gedeelde inhoud", "Url", "Gedeeld via", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out


def settings_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Settings.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Interests: (.*?)$", re.MULTILINE)
        match = re.search(pattern, text)
        if match:
            interests = match.group(1).split("|")
            out = pd.DataFrame(interests, columns=["Interesses"])

    except Exception as e:
        logger.error(e)

    return out


def block_list_to_df(tiktok_zip: str):

    out = pd.DataFrame()

    try:
        b = unzipddp.extract_file_from_zip(tiktok_zip, "Block List.txt")
        b = io.TextIOWrapper(b, encoding='utf-8')
        text = b.read()

        pattern = re.compile(r"^Date: (.*?)\nUsername: (.*?)$", re.MULTILINE)
        matches = re.findall(pattern, text)
        out = pd.DataFrame(matches, columns=["Tijdstip", "Gebruikersnaam"])
        out = out[["Gebruikersnaam", "Tijdstip"]]

    except Exception as e:
        logger.error(e)

    return out

