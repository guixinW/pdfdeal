import asyncio
import logging
from typing import Dict, List, Optional, Union
from .Doc2X.ConvertV2 import parse_image_layout, parse_image_ocr
from .Doc2X.Exception import RateLimit, run_async
from .FileTools.file_tools import get_files
import os

logger = logging.getLogger("pdfdeal.doc2x_img")


class ImageProcessor:
    """Image processor with rate limiting support"""

    def __init__(self, apikey: str):
        """Initialize the image processor

        Args:
            apikey (str): API key for authentication
        """
        self.apikey = apikey
        self._request_times: List[float] = []
        self._lock = asyncio.Lock()

    async def _check_rate_limit(self):
        """Check and enforce rate limit (120 requests per 30 seconds)"""
        async with self._lock:
            current_time = asyncio.get_event_loop().time()
            # Remove requests older than 30 seconds
            self._request_times = [
                t for t in self._request_times if current_time - t <= 30
            ]

            if len(self._request_times) >= 120:
                # Calculate wait time if rate limit reached
                wait_time = 30 - (current_time - self._request_times[0])
                if wait_time > 0:
                    logger.warning(
                        f"Rate limit reached, waiting for {wait_time:.2f} seconds"
                    )
                    await asyncio.sleep(wait_time)
                    # Recursive call after waiting to ensure we're still within limits
                    await self._check_rate_limit()

            self._request_times.append(current_time)

    async def process_image(
        self, image_path: str, process_type: str = "ocr", zip_path: str = None
    ) -> tuple[list, str, bool]:
        """Process an image with OCR or layout analysis

        Args:
            image_path (str): Path to the image file
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            zip_path (str, optional): Path to save the zip file for layout analysis. Defaults to None.

        Returns:
            Tuple containing:
                - The processing result (list of tex_lines for OCR, list of pages for layout)
                - The uid of the processed image
                - Boolean indicating if the processing was successful

        Raises:
            ValueError: If process_type is invalid or file type is not supported
            RateLimit: If rate limit is exceeded
        """
        if process_type not in ["ocr", "layout"]:
            raise ValueError("process_type must be one of: 'ocr', 'layout'")

        try:
            if process_type == "ocr":
                await self._check_rate_limit()
                tex_lines, uid = await parse_image_ocr(self.apikey, image_path)
                return tex_lines, uid, True
            else:
                await self._check_rate_limit()
                pages, uid = await parse_image_layout(self.apikey, image_path, zip_path)
                return pages, uid, True

        except RateLimit as e:
            logger.error(f"Rate limit exceeded: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing image {image_path}: {str(e)}")
            return [], "", False

    async def process_multiple_images(
        self,
        image_paths: List[str],
        process_type: str = "ocr",
        concurrent_limit: int = 5,
        zip_path: str = None,
    ) -> tuple[List[list], Dict[str, bool]]:
        """Process multiple images concurrently with rate limiting

        Args:
            image_paths (List[str]): List of image file paths
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            concurrent_limit (int): Maximum number of concurrent processing tasks
            zip_path (str, optional): Path to save the zip file for layout analysis. Defaults to None.

        Returns:
            Tuple containing:
                - List of processing results in order (empty list for failed items)
                - Dict mapping image paths to their success status
        """
        semaphore = asyncio.Semaphore(concurrent_limit)

        async def process_with_semaphore(
            path: str,
            index: int,
        ) -> tuple[int, str, tuple[list, str, bool]]:
            async with semaphore:
                result = await self.process_image(path, process_type, zip_path)
                return index, path, result

        tasks = [process_with_semaphore(path, i) for i, path in enumerate(image_paths)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = [[] for _ in range(len(image_paths))]
        success_status = {}

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Failed to process a file: {str(result)}")
                continue

            index, path, (result_list, _, success) = result
            processed_results[index] = result_list if success else []
            success_status[path] = success

        return processed_results, success_status

    async def pic2file_back(
        self,
        pic_file,
        process_type: str = "ocr",
        concurrent_limit: Optional[int] = None,
        zip_path: str = None,
    ) -> tuple[List[Union[list, str]], List[dict], bool]:
        """Process image files with OCR or layout analysis

        Args:
            pic_file (str | List[str]): Path to image file(s) or directory
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            concurrent_limit (int, optional): Maximum number of concurrent tasks. Defaults to None.
            zip_path (str, optional): Path to save the zip file for layout analysis. Defaults to None.

        Returns:
            Tuple containing:
                - List of results in order (empty string for failed items)
                - List of dictionaries containing error information
                - Boolean indicating if any errors occurred
        """
        if isinstance(pic_file, str):
            if os.path.isdir(pic_file):
                pic_file, _ = get_files(path=pic_file, mode="pic")
            else:
                pic_file = [pic_file]

        results, success_status = await self.process_multiple_images(
            image_paths=pic_file,
            process_type=process_type,
            concurrent_limit=concurrent_limit or 5,
            zip_path=zip_path,
        )

        failed_files = []
        has_error = False

        # Convert results to final format
        final_results = []
        for i, path in enumerate(pic_file):
            if not success_status.get(path, False):
                failed_files.append({"error": "Processing failed", "path": path})
                final_results.append("")
                has_error = True
            else:
                failed_files.append({"error": "", "path": ""})
                final_results.append(results[i])

        if has_error:
            logger.error(
                f"Failed to process {len([f for f in failed_files if f['error']])} file(s)"
            )

        return final_results, failed_files, has_error

    def pic2file(
        self,
        pic_file,
        process_type: str = "ocr",
        concurrent_limit: Optional[int] = None,
        zip_path: str = None,
    ) -> tuple[List[Union[list, str]], List[dict], bool]:
        """Synchronous wrapper for pic2file_back

        Args:
            pic_file (str | List[str]): Path to image file(s) or directory
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            concurrent_limit (int, optional): Maximum number of concurrent tasks. Defaults to None.
            zip_path (str, optional): Path to save the zip file for layout analysis. Defaults to None.

        Returns:
            Same as pic2file_back
        """
        return run_async(
            self.pic2file_back(
                pic_file=pic_file,
                process_type=process_type,
                concurrent_limit=concurrent_limit,
                zip_path=zip_path,
            )
        )
