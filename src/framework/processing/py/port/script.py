import logging
import json
import io
from typing import Tuple

import pandas as pd

import port.api.props as props
import port.helpers as helpers
import port.validate as validate
import port.youtube as youtube
import port.tiktok as tiktok

from port.api.commands import (CommandSystemDonate, CommandUIRender, CommandSystemExit)

LOG_STREAM = io.StringIO()

# If you uncomment this line log will be streamed to a buffer
# this buffer can be read out and "donated" (stored) as a file
# and can be inspected during research
# donate_logs() will not donate any logs if the buffer isn't used

logging.basicConfig(
    stream=LOG_STREAM, # if you uncomment this line, logs will be send to buffer
    level=logging.DEBUG,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger("script")


# In process() you will find the object donation_dict
# This is not something I usually do, but is needed for tiktok
# If you check the code you can see that donation_dict is only used with tiktok
# donation_dict is used to limit the number records participants get to see for review
# if donation_dict is not none the values in donation_dict will be donated instead of the tables themselves

def process(session_id):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{session_id}-tracking")

    platforms = [("YouTube", extract_youtube, youtube.validate_zip), 
                 ("TikTok", extract_tiktok, tiktok.validate_zip)]

    #platforms = [ ("YouTube", extract_youtube, youtube.validate_zip), ]
    #platforms = [ ("TikTok", extract_tiktok, tiktok.validate_zip), ]

    # For each platform
    # 1. Prompt file extraction loop
    # 2. In case of succes render data on screen
    for platform in platforms:
        platform_name, extraction_fun, validation_fun = platform

        table_list = None
        donation_dict = None

        # Prompt file extraction loop
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{session_id}-{platform_name}-tracking")

            # Render the propmt file page
            promptFile = prompt_file("application/zip, text/plain, application/json", platform_name)
            file_result = yield render_page(platform_name, promptFile)

            if file_result.__type__ == "PayloadString":
                validation = validation_fun(file_result.value)

                # DDP is recognized: Status code zero
                if validation.status_code.id == 0: 
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{session_id}-{platform_name}-tracking")

                    table_list, donation_dict = extraction_fun(file_result.value, validation)
                    break

                # DDP is not recognized: Different status code
                if validation.status_code.id != 0: 
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{session_id}-{platform_name}-tracking")
                    retry_result = yield render_page(platform_name, retry_confirmation(platform_name))

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{session_id}-{platform_name}-tracking")
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{session_id}-{platform_name}-tracking")
                break

        # Render tables on screen
        if table_list is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{session_id}-{platform_name}-tracking")

            # Check if something got extracted
            if len(table_list) == 0:
                yield donate_status(f"{session_id}-{platform_name}-NO-DATA-FOUND", "NO_DATA_FOUND")
                table_list.append(create_empty_table(platform_name))

            prompt = assemble_tables_into_form(table_list)
            consent_result = yield render_page(platform_name, prompt)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                if donation_dict is not None:
                    yield from donate_dict(platform_name, donation_dict)
                else:
                    yield donate(platform_name, consent_result.value)
                yield donate_logs(f"{session_id}-{platform_name}-tracking")
                yield donate_status(f"{session_id}-{platform_name}-DONATED", "DONATED")

            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{session_id}-{platform_name}-tracking")
                yield donate_status(f"{session_id}-{platform_name}-SKIP-REVIEW-CONSENT", "SKIP_REVIEW_CONSENT")

    yield exit(0, "Success")
    yield render_end_page()


##################################################################
# Functions that define the extraction logic

def extract_youtube(youtube_zip: str, validation: validate.ValidateInput) -> Tuple[list[props.PropsUIPromptConsentFormTable], dict]:
    """
    Main data extraction function for youtube
    Assemble all extraction logic here
    """
    tables_to_render = []
    donation_dict = None

    df = youtube.watch_history_to_df(youtube_zip, validation)
    df.columns = ['Titel', 'Url', 'Reklame', 'Kanaal', 'Tijdstip']
    # For wordcloud, workaround to not show "null" (downside: affects font size scale)
    df["Kanaal"] = df["Kanaal"].fillna("")
    if not df.empty:
        table_title = props.Translatable({
            "en": "Your YouTube watch history",
            "nl": "Je YouTube kijkgeschiedenis",
        })
        table_description = props.Translatable({
            "en": "In this table you find the videos you watched on YouTube sorted over time. Below, you find visualizations of different parts of this table.", 
            "nl": "Hieronder vind je een overzicht van de video's die je hebt bekeken en wanneer dit was. *Uit deze lijst gaan we enkel onderzoeken of je video's hebt bekeken die gaan over gokken en wedden.",
        })
        wordcloud = {
            "title": {
                "en": "The most frequently watched YouTube channels. The size of the words represents how frequently you viewed YouTube channels.", 
                "nl": "De meest bekeken YouTube-kanalen. De grootte van de woorden geeft weer hoe vaak je YouTube-kanalen hebt bekeken.", 
            },
            "type": "wordcloud",
            "textColumn": "Kanaal",
            "tokenize": False,
        }

        total_watched = {
            "title": {
                "en": "The total number of YouTube videos you have watched per month.", 
                "nl": "Hieronder vind je een grafiek die laat zien hoeveel video's je elke maand hebt bekeken.", 
            },
            "type": "area",
            "group": {
                "column": "Tijdstip",
                "dateFormat": "month"
            },
            "values": [{
                "aggregate": "count", 
                "label": {
                    "en": "number of views", 
                    "nl": "aantal keer gekeken"
                }
            }]
        }

        hour_of_the_day = {
            "title": {
                "en": "The total number of YouTube videos you have watched per hour of the day.", 
                "nl": "Hieronder vind je een grafiek die laat zien hoeveel video's je hebt bekeken per uur van de dag.", 
            },
            "type": "bar",
            "group": {
                "column": "Tijdstip",
                "dateFormat": "hour_cycle"
            },
            "values": [{}]
        }

        table = props.PropsUIPromptConsentFormTable("youtube_watch_history", table_title, df, table_description, [total_watched, wordcloud, hour_of_the_day])
        tables_to_render.append(table)

    df = youtube.search_history_to_df(youtube_zip, validation)
    df.columns = ['Zoekterm', 'Url', 'Tijdstip']
    if not df.empty:
        table_title = props.Translatable({
            "en": "Your YouTube search history",
            "nl": "Je YouTube-zoekgeschiedenis",
        })
        table_description = props.Translatable({
            "en": "In this table you find the search terms you have used on YouTube sorted over time.", 
            "nl": "Hieronder vind je een overzicht van de zoektermen die je hebt gebruikt en wanneer dit was. De zoektermen zijn gesorteerd op tijd. *Uit deze lijst gaan we enkel onderzoeken of je zoektermen die gaan over gokken en wedden hebt gebruikt.", 
        })
        wordcloud = {
            "title": {
                "en": "Words you most searched for. The size of the words represents how frequently you used a search term.", 
                "nl": "Zoektermen die je het vaakst hebt gebruikt. De grootte van de woorden in de grafiek geven aan hoe vaak je een zoekterm hebt gebruikt.", 
            },
            "type": "wordcloud",
            "textColumn": "Zoekterm",
            "tokenize": True,
        }
        table = props.PropsUIPromptConsentFormTable("youtube_search_history", table_title, df, table_description, [wordcloud])
        tables_to_render.append(table)

    df = youtube.subscriptions_to_df(youtube_zip, validation)
    df = df[["Kanaaltitel","Kanaal-URL","Kanaal-ID"]]
    if not df.empty:
        table_title = props.Translatable({
            "en": "Your YouTube channel subscriptions",
            "nl": "Je YouTube-kanaal abonnementen",
        })
        table_description = props.Translatable({
            "en": "In this table, you find the YouTube channels you are subscribed to.", 
            "nl": "Hieronder vind je een overzicht van de YouTube kanalen waarop je geabonneerd bent. *Uit deze lijst gaan we enkel onderzoeken of je YouTube kanalen die berichten over gokken en wedden plaatsen volgt.", 
        })
        table = props.PropsUIPromptConsentFormTable("idasjdhj1", table_title, df, table_description, [])
        tables_to_render.append(table)

    return (tables_to_render, donation_dict)



def extract_tiktok(tiktok_file: str, validation) -> Tuple[list[props.PropsUIPromptConsentFormTable], dict]:
    tables_to_render = []
    donation_dict = {}

    df = tiktok.browsing_history_to_df(tiktok_file)
    if not df.empty:
        # We only render the first chunk as table, but donate everything
        dfs = helpers.split_dataframe(df, 250000)
        for i, df in enumerate(dfs):
            df_name = f"tiktok_video_browsing_history_{i}"
            table_title = props.Translatable({"en": "Watch history", 
                                              "nl": "Video's die je hebt bekeken"})
            table_description = props.Translatable(
                {
                    "en": "The table below shows exactly which TikTok videos you watched and when that was. Do you have exactly 250000 rows in the table? Then we couldn't show all your data in this table. Are you curious about the rest? Open the zip file, go to 'Activity' and open 'Browsing history.txt'. Then you can see the rest for yourself. Can't find it? Let us know on WhatsApp.",
                    "nl": "Hieronder vind je een overzicht van de video's die je hebt bekeken en wanneer dit was. Heb je precies 250000 rijen in de tabel zitten? Dat konden we niet al je data laten zien in deze tabel. Ben je benieuwd naar de rest? Open de zipfile, ga naar 'Activity' en open 'Browsing history.txt'. Dan kun je zelf de rest bekijken. *Uit deze lijst gaan we enkel onderzoeken of je video's hebt bekeken die gaan over gokken en wedden.",
                 }
            )
            hours_logged_in = {
                "title": {"en": "Total number of videos watched per month", 
                          "nl": "Hieronder vind je een grafiek die laat zien hoeveel video's je elke maand hebt bekeken."},
                "type": "area",
                "group": {
                    "column": "Tijdstip",
                    "dateFormat": "month"
                },
                "values": [{
                    "label": "Aantal"
                }]
            }
            # only the first df chunk is shown as table 
            if i == 0:
                table = props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description, [hours_logged_in]) 
                tables_to_render.append(table)

            donation_dict[df_name] = df.to_dict(orient="records")

    df = tiktok.favorite_videos_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_favorite_videos"
        table_title = props.Translatable(
            {
                "en": "Favorite videos", 
                "nl": "Video's die je hebt opgeslagen", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "In the table below you will find the videos that are among your favorites.", 
                "nl": "Hieronder vind je een overzicht van de video's die je hebt opgeslagen en wanneer dit was. *Uit deze lijst gaan we enkel onderzoeken of je video's hebt opgeslagen die gaan over gokken en wedden.", 
             }
        )
        table = props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")

    df = tiktok.like_list_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_like_list"
        table_title = props.Translatable(
            {
                "en": "Videos you have liked", 
                "nl": "Video's die je hebt geliket", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "The table below shows the videos you've liked and when that was.",
                "nl": "Hieronder vind je een overzicht van de video's die je hebt geliket en wanneer dit was. *Uit deze lijst gaan we enkel onderzoeken of je video's hebt geliket die gaan over gokken en wedden.",
             }
        )
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")

    df = tiktok.share_history_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_share_history"
        table_title = props.Translatable(
            {
                "en": "Shared videos", 
                "nl": "Video's die je hebt gedeeld", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "The table below shows what you shared, at what time and how.",
                "nl": "Hieronder vind je een overzicht van de video's die je hebt gedeeld, wanneer dit was en de manier waarop je het hebt gedeeld. *Uit deze lijst gaan we enkel onderzoeken of je video's hebt gedeeld die gaan over gokken en wedden.",
             }
        )
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")
    
    df = tiktok.favorite_hashtag_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_favorite_hashtags"
        table_title = props.Translatable(
            {
                "en": "Favorite hashtags", 
                "nl": "Favoriete hashtags", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "The table below lists the hashtags that are among your favorites.", 
                "nl": "Hieronder vind je de hashtags die tot je favorieten behoren.", 
             }
        )
        table = props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")


    df = tiktok.searches_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_searches"
        table_title = props.Translatable(
            {
                "en": "Search terms", 
                "nl": "Zoektermen die je hebt gebruikt", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "The chart below shows what you searched for and when that was.",
                "nl": "Hieronder vind je een overzicht van de zoektermen die je hebt gebruikt en wanneer dit was. *Uit deze lijst gaan we enkel onderzoeken of je zoektermen die gaan over gokken en wedden hebt gebruikt.",
             }
        )
        wordcloud = {
            "title": {"en": "The size of the words in the chart indicates how often the search term appears in your data.", 
                      "nl": "Zoektermen die je het vaakst hebt gebruikt. De grootte van de woorden in de grafiek geven aan hoe vaak je een zoekterm hebt gebruikt."},
            "type": "wordcloud",
            "textColumn": "Zoekterm",
        }
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description, [wordcloud])
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")

    df = tiktok.follower_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_followers"
        table_title = props.Translatable(
            {
                "en": "Followers", 
                "nl": "Accounts die jou volgen", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "The table below shows your followers and when they started following you.",
                "nl": "Hieronder vind je een overzicht van de accounts die jou volgen en de datum waarop ze zijn gestart met jou te volgen. *Uit deze lijst gaan we enkel onderzoeken of er accounts die berichten over gokken en wedden plaatsen jou volgen.",
             }
        )
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")

    df = tiktok.following_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_following"
        table_title = props.Translatable(
            {
                "en": "Following", 
                "nl": "Accounts die je volgt", 
            }
        )
        table_description = props.Translatable(
            {
                "en": "The table below shows users you follow and the time you started following them.",
                "nl": "Hieronder vind je een overzicht van de accounts die je volgt en de datum waarop je ze bent gaan volgen. *Uit deze lijst gaan we enkel onderzoeken of je accounts die berichten over gokken en wedden plaatsen volgt.",
             }
        )
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")
    
    df = tiktok.block_list_to_df(tiktok_file)
    if not df.empty:
        df_name = "tiktok_block_list"
        table_title = props.Translatable(
            {
                "en": "Blocked accounts on TikTok", 
                "nl": "Accounts die je blokeert"
                }
        )
        table_description = props.Translatable(
            {
                "en": "Below are users you block.",
                "nl": "Hieronder vind je accounts die je blokeert. *Uit deze lijst gaan we enkel onderzoeken of je accounts die berichten over gokken en wedden plaatsen blokeert.",
             }
        )
        table =  props.PropsUIPromptConsentFormTable(df_name, table_title, df, table_description)
        tables_to_render.append(table)
        donation_dict[df_name] = df.to_dict(orient="records")
    

    return (tables_to_render, donation_dict)



####################################################################
# script.py helpers

def assemble_tables_into_form(table_list: list[props.PropsUIPromptConsentFormTable]) -> props.PropsUIPromptConsentForm:
    """
    Assembles all donated data in consent form to be displayed
    """
    return props.PropsUIPromptConsentForm(table_list, [])


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream
    if log_string:
        log_data = log_string.split("\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


def create_empty_table(platform_name: str) -> props.PropsUIPromptConsentFormTable:
    """
    Show something in case no data was extracted
    """
    title = props.Translatable({
       "en": "Nothing went wrong, but we couldn't find any data in your files",
       "nl": "Er ging niks mis, maar we konden geen gegevens in jouw data vinden",
    })
    df = pd.DataFrame(["No data found"], columns=["No data found"])
    table = props.PropsUIPromptConsentFormTable(f"{platform_name}_no_data_found", title, df)
    return table


def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_page(platform, body):
    header = props.PropsUIHeader(props.Translatable(
        {
            "en": platform, 
            "nl": "Je " + platform + " geschiedenis"
        }
    ))
    footer = props.PropsUIFooter()
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we could not process your {platform} file. If you are sure that you selected the correct file, press Continue. To select a different file, press Try again.",
            "nl": f"Helaas, kunnen we je {platform} bestand niet verwerken. Weet je zeker dat je het juiste bestand hebt gekozen? Ga dan verder. Probeer opnieuw als je een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions, platform):
    description = props.Translatable(
        {
            "en": f"Please follow the download instructions and choose the file that you stored on your device. Click “Skip” at the right bottom, if you do not have a file from {platform}.",
            "nl": f"Volg de download instructies en kies het bestand dat je opgeslagen hebt op je apparaat. Als je geen {platform} bestand hebt klik dan op “Overslaan” rechts onder."
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit(code, info):
    return CommandSystemExit(code, info)


def donate_status(filename: str, message: str):
    return donate(filename, json.dumps({"status": message}))


def donate_dict(platform_name: str, d: dict):
    for k, v in d.items():
        donation_str = json.dumps({k: v})
        yield donate(f"{platform_name}_{k}", donation_str)

