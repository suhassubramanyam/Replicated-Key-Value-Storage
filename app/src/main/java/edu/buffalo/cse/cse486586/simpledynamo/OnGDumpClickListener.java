package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * Created by suhas on 5/1/16.
 */

import android.content.ContentResolver;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.util.Log;
import android.view.View;
import android.widget.TextView;

public class OnGDumpClickListener implements View.OnClickListener {

    private static final String TAG = OnGDumpClickListener.class.getName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    private static final String STAR_SIGN = "*";

    private final TextView mTextView;
    private final ContentResolver mContentResolver;
    private final Uri mUri;

    public OnGDumpClickListener(TextView _tv, ContentResolver _cr) {
        mTextView = _tv;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public void onClick(View v) {
        new Task().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
    }

    private class Task extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            Cursor resultCursor = mContentResolver.query(mUri, null,
                    STAR_SIGN, null, null);
            try {
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }
                else{
                    int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                    Log.d(TAG, "doInBackground: keyIndex: "+keyIndex);

                    int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
                    Log.d(TAG, "doInBackground: valueIndex: "+valueIndex);

                    while(resultCursor.moveToNext()){
                        publishProgress("key: "+resultCursor.getString(keyIndex)+"\n"+"value: "+resultCursor.getString(valueIndex)+"\n");
                        Log.d(TAG, "doInBackground: resultCursor key: " + resultCursor.getString(keyIndex) + "resultCursor value: " + resultCursor.getString(valueIndex));
                    }
                }
            }
            catch (Exception ex){
                ex.printStackTrace();
            }
            finally {
                if(resultCursor != null) {
                    resultCursor.close();
                }
            }

            return null;
        }

        protected void onProgressUpdate(String...strings) {
            mTextView.append(strings[0]);
        }

    }
}
