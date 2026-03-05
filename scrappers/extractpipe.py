import logging
import pandas as pd
from utils.data_utils import write_csv_file

logger = logging.getLogger(__name__)


class ExportPipe:
    def __init__(self):
        self.records = []

    def open_spider(self, spider):
        self.records = []
        logger.info("Spider opened.")

    def process_item(self, item, spider):
        self.records.append(dict(item))
        return item

    def close_spider(self, spider):
        if self.records:
            df = pd.DataFrame(self.records)
            write_csv_file(df, "extract_data.csv")
            logger.info(
                "Saved %d records to extract_data.csv", len(self.records)
            )
        else:
            logger.warning("No records were scraped.")