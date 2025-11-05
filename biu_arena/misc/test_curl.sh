#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
export AWS_DEFAULT_REGION=$(aws configure get region)

region=${AWS_DEFAULT_REGION}
user_password=${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}

# url="https://biu-velox.s3.us-east-1.amazonaws.com/velox-tpch-data/sf-0.01/lineitem/lineitem-1.parquet"
# curl -s $url --aws-sigv4 "aws:amz:${region}:s3" --user "${user_password}" \
# -o /dev/null -w "%{http_code}\n" -v

# url="https://biu-velox.s3.us-east-1.amazonaws.com/velox-tpch-data/sf-0.01/lineitem/lineitem-1.parquet?response-content-disposition=inline&X-Amz-Content-Sha256=UNSIGNED-PAYLOAD&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELX%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIDCSSV2LENu9rguxw39g8RuDhrsyK0m64Skqwehy%2B0S8AiEAuxcrtiiDsskUc8qp9azeWL0gw0dmD7SV62umAQaymeUquQMIfhADGgw0ODgwMDg5NTI1MDAiDFsmSTNlGn9CtdajcyqWA90DCWCCcMXwXir06RE2HATkEgEY4XzS%2B2Me3Lpu8xloE3%2BbVuJRKdHekB6XxmTK7fN1GOgglOlXd5USHrXkVERG19Ch3gnuhGIHtHYzHiJsTGLI5Lb%2BJG%2BkUnG309C%2F4AeNoi3jXiQzrTVy%2FmzYm02YCrmLcHFPKm9deCFmv27kQ5HGWlp7W4mBm2SpH2L%2BoU%2Ft7W6%2B5igvM2NYcsk3f5WnCaUzg4xfTWd1jZHUJAFF%2FfTZsluGinJXewARnAlb1v0rUvCwmxuE7XiQbq9ilnEIHv881lyNH5xWFQYmb0k4lS4ebsFl%2BQxIxVu6SrI2MRfHDIjCtCuuGH%2FU70y2I5N2SQ0sWDJ3%2FLnhf0q3I%2B3uS9uIyRSkfOU9kHyWVBgowjN3RfBG1eFZTRZx1AoIbOW62v9pcJgfn4ILyWl6E8Fw4u%2BTfu5Hau8XLHTza4f3jdRV1EC0P%2BE5gSqOAo1y7LlXeO22NEM5J8xaV56us%2FcU%2B%2BKWw8fgQEbGPHn1vnEzl7WzZQ3d4%2BTinxKy2FnxT2f1VE0EjKkwr7WpyAY63gIZyA8XnB4H%2FfFUYVx9AnszDx859PzwNIXTfAsW1rA0OnpUBJB%2FFfEz5h2Lz9mxvxqbNsrEfTKzgSR3ewNaZ53euc%2FZAB7FIlX5vOghhKNvCzQCTp%2FZkhO6SYkbuxM%2B4O5gOuz7gwPUHl6hPZrXifkdOXLyVoWayD%2BYxDAhbOiogDot98CyAnJX81O3agJk7gSu0ZnEWBX2L%2BQsBI%2F7gz09rXWjS%2FX9Nvrg%2BiqMuCJtFJ3j9aIJDlZBhF3DqkViYS3xaRL3Qx9M2L9p3fNcIy5ckEka0YPadGCO3ir3IDSrwCk%2FdtzQzEYkoX5cyiqJyrAas5QAhfFC43xQvrpJpXyvwwWljc%2FX9wOSBJAdHQv2Vsv9xdZenqm85rP5UEd9utbhlgIdTnUg91ybxgyIzqksuBr6aFxOZkz8PNmiuro5FRb3CCyT2Mu31xSILKy6163C1udUaMwaP8m5tlsDHg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAXDH4ZVK2NOATXA2W%2F20251104%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20251104T210230Z&X-Amz-Expires=600&X-Amz-SignedHeaders=host&X-Amz-Signature=fa1f432a16267feb57d4310002f4356df567a6a09b6f82acab1898b32a47f5ca"
# curl -s $url \
# -o /dev/null -w "%{http_code}\n" -v

url="https://biu-velox.s3.us-east-1.amazonaws.com/velox-tpch-data/sf-0.01/lineitem/lineitem-1.parquet"
curl -s $url \
-o /dev/null -w "%{http_code}\n" -v
