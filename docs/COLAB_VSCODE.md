# Colab + VS Code Setup

## 1) Install VS Code Extensions

### UI flow
1. Open VS Code.
2. Open Extensions panel.
3. Install:
   - Python (`ms-python.python`)
   - Jupyter (`ms-toolsai.jupyter`)
   - Google Colab extension (if available in your marketplace).

### CLI flow (PowerShell)
```powershell
code --install-extension ms-python.python
code --install-extension ms-toolsai.jupyter
code --install-extension Google.colab
```

## 2) Fix "No assigned colab servers"
1. Open any `.ipynb` file in VS Code.
2. Click **Select Kernel** (top-right of notebook).
3. Choose **Colab**.
4. Choose **New Colab Server**.
5. Complete Google auth prompt.
6. Re-open kernel picker and select the newly attached Colab runtime.

## 3) Earth Engine Auth in Colab
Use this in notebook setup cells:
```python
import ee

ee.Authenticate()
ee.Initialize(project="YOUR_GCP_PROJECT_ID")
```

## 4) Recommended Drive Structure
```
MyDrive/
  gee-livestock/
    exports/
      geotiff/
      zarr/
      logs/
    notebooks/
```

## 5) Practical Notes
- Keep long-running GEE jobs in Colab.
- Keep local VS Code usage focused on editing and light verification.
- Prefer GCS for very large exports and repeatable pipelines.
