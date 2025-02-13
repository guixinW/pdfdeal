import asyncio
import logging
from typing import Dict, List, Union, Optional
from .Doc2X.ConvertV2 import parse_image_layout, parse_image_ocr
from .Doc2X.Exception import RateLimit, RequestError, run_async
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
        self, image_path: str, process_type: str = "ocr"
    ) -> Dict[str, Union[dict, str]]:
        """Process an image with OCR or layout analysis

        Args:
            image_path (str): Path to the image file
            process_type (str): Type of processing, can be 'ocr' or 'layout'

        Returns:
            Dict containing the processing results

        Raises:
            ValueError: If process_type is invalid
            RateLimit: If rate limit is exceeded
        """
        if process_type not in ["ocr", "layout"]:
            raise ValueError("process_type must be one of: 'ocr', 'layout'")

        result = {}

        try:
            if process_type == "ocr":
                await self._check_rate_limit()
                ocr_result = await parse_image_ocr(self.apikey, image_path)
                result["ocr"] = ocr_result
            else:
                await self._check_rate_limit()
                layout_result = await parse_image_layout(self.apikey, image_path)
                result["layout"] = layout_result

            return result

        except RateLimit as e:
            logger.error(f"Rate limit exceeded: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing image {image_path}: {str(e)}")
            raise

    async def process_multiple_images(
        self,
        image_paths: List[str],
        process_type: str = "ocr",
        concurrent_limit: int = 5,
    ) -> Dict[str, Dict[str, Union[dict, str]]]:
        """Process multiple images concurrently with rate limiting

        Args:
            image_paths (List[str]): List of image file paths
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            concurrent_limit (int): Maximum number of concurrent processing tasks

        Returns:
            Dict mapping image paths to their processing results
        """
        semaphore = asyncio.Semaphore(concurrent_limit)

        async def process_with_semaphore(path: str) -> tuple[str, dict]:
            async with semaphore:
                result = await self.process_image(path, process_type)
                return path, result

        tasks = [process_with_semaphore(path) for path in image_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed_results = {}
        for path, result in results:
            if isinstance(result, Exception):
                logger.error(f"Failed to process {path}: {str(result)}")
                processed_results[path] = {"error": str(result)}
            else:
                processed_results[path] = result

        return processed_results

    async def pic2file_back(
        self,
        pic_file,
        process_type: str = "ocr",
        concurrent_limit: Optional[int] = None,
    ) -> tuple[Dict[str, Dict[str, Union[dict, str]]], List[dict], bool]:
        """Process image files with OCR or layout analysis

        Args:
            pic_file (str | List[str]): Path to image file(s) or directory
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            concurrent_limit (int, optional): Maximum number of concurrent tasks. Defaults to None.

        Returns:
            Tuple containing:
                - Dictionary mapping file paths to their processing results
                - List of dictionaries containing error information
                - Boolean indicating if any errors occurred
        """
        if isinstance(pic_file, str):
            if os.path.isdir(pic_file):
                pic_file, _ = get_files(path=pic_file, mode="pic")
            else:
                pic_file = [pic_file]

        results = await self.process_multiple_images(
            image_paths=pic_file,
            process_type=process_type,
            concurrent_limit=concurrent_limit or 5,
        )

        failed_files = []
        has_error = False

        for path, result in results.items():
            if "error" in result:
                failed_files.append({"error": result["error"], "path": path})
                has_error = True
            else:
                failed_files.append({"error": "", "path": ""})

        if has_error:
            logger.error(
                f"Failed to process {len([f for f in failed_files if f['error']])} file(s)"
            )

        return results, failed_files, has_error

    def pic2file(
        self,
        pic_file,
        process_type: str = "ocr",
        concurrent_limit: Optional[int] = None,
    ) -> tuple[Dict[str, Dict[str, Union[dict, str]]], List[dict], bool]:
        """Synchronous wrapper for pic2file_back

        Args:
            pic_file (str | List[str]): Path to image file(s) or directory
            process_type (str): Type of processing, can be 'ocr' or 'layout'
            concurrent_limit (int, optional): Maximum number of concurrent tasks. Defaults to None.

        Returns:
            Same as pic2file_back
        """
        return run_async(
            self.pic2file_back(
                pic_file=pic_file,
                process_type=process_type,
                concurrent_limit=concurrent_limit,
            )
        )
